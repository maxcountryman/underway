-- Force anything running this migration to use the right search path.
set local search_path to underway;

-- Introduce a waiting state for tasks that are suspended on activity calls.
alter type underway.task_state add value if not exists 'waiting' after 'in_progress';

do $$
begin
    create type underway.activity_call_state as enum (
        'pending',
        'in_progress',
        'succeeded',
        'failed'
    );
exception
    when duplicate_object then null;
end
$$;

create table if not exists underway.activity_call (
    id              uuid not null primary key,
    task_queue_name text not null,
    job_id          uuid not null,
    step_index      integer not null,
    call_key        text not null,
    activity        text not null,
    input           jsonb not null,
    output          jsonb,
    error           jsonb,
    state           underway.activity_call_state not null default 'pending',
    attempt_count   integer not null default 0,
    available_at    timestamp with time zone not null default now(),
    created_at      timestamp with time zone not null default now(),
    updated_at      timestamp with time zone not null default now(),
    started_at      timestamp with time zone,
    completed_at    timestamp with time zone,
    unique (task_queue_name, job_id, step_index, call_key)
);

create index if not exists idx_activity_call_pending
on underway.activity_call (state, available_at, created_at)
where state = 'pending';

create index if not exists idx_activity_call_workflow
on underway.activity_call (task_queue_name, job_id, step_index, state);

create or replace function underway.activity_call_change_notify()
returns trigger as $$
begin
    if (new.state = 'pending') then
        perform pg_notify(
            'activity_call_change',
            json_build_object(
                'task_queue_name', new.task_queue_name,
                'job_id', new.job_id,
                'step_index', new.step_index,
                'call_key', new.call_key
            )::text
        );
    end if;

    return new;
end;
$$ language plpgsql;

drop trigger if exists activity_call_changed on underway.activity_call;

create trigger activity_call_changed
after insert or update on underway.activity_call
for each row
execute procedure underway.activity_call_change_notify();
