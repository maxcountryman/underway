-- Force anything running this migration to use the right search path.
set local search_path to underway;

-- Task execution lease fields.
alter table underway.task
    add column if not exists run_at timestamp with time zone not null default now(),
    add column if not exists attempt_count integer not null default 0,
    add column if not exists current_attempt integer not null default 0,
    add column if not exists lease_expires_at timestamp with time zone;

-- Populate run_at from existing delay semantics.
update underway.task
set run_at = coalesce(last_attempt_at, created_at) + delay;

-- Backfill attempt counters from historical attempt rows.
with attempts as (
    select
        task_queue_name,
        task_id,
        max(attempt_number) as max_attempt
    from underway.task_attempt
    group by task_queue_name, task_id
)
update underway.task t
set attempt_count = coalesce(a.max_attempt, 0),
    current_attempt = coalesce(a.max_attempt, 0)
from attempts a
where t.task_queue_name = a.task_queue_name
  and t.id = a.task_id;

-- Backfill lease expiry for currently in-progress tasks.
update underway.task
set lease_expires_at = coalesce(last_heartbeat_at, updated_at, created_at) + heartbeat
where state = 'in_progress'::underway.task_state;

alter table underway.task
    add constraint task_attempt_counters_valid
    check (attempt_count >= current_attempt and current_attempt >= 0);

create index if not exists idx_task_claim_pending
on underway.task (task_queue_name, priority desc, run_at, id)
where state = 'pending'::underway.task_state;

create index if not exists idx_task_claim_stale
on underway.task (task_queue_name, lease_expires_at, priority desc, id)
where state = 'in_progress'::underway.task_state;

-- Activity-call execution lease fields.
alter table underway.activity_call
    add column if not exists timeout_ms integer not null default 900000,
    add column if not exists max_attempts integer not null default 5,
    add column if not exists lease_expires_at timestamp with time zone;

update underway.activity_call
set lease_expires_at =
    coalesce(started_at, updated_at, created_at)
    + ((timeout_ms + 30000) * interval '1 millisecond')
where state = 'in_progress'::underway.activity_call_state;

create index if not exists idx_activity_call_claim_pending
on underway.activity_call (task_queue_name, available_at, created_at, id)
where state = 'pending'::underway.activity_call_state;

create index if not exists idx_activity_call_claim_stale
on underway.activity_call (task_queue_name, lease_expires_at, created_at, id)
where state = 'in_progress'::underway.activity_call_state;
