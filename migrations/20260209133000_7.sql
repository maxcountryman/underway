-- Force anything running this migration to use the right search path.
set local search_path to underway;

create table if not exists underway.activity_call_attempt (
    activity_call_id uuid not null,
    attempt_number   integer not null,
    state            underway.activity_call_state not null,
    error            jsonb,
    started_at       timestamp with time zone not null default now(),
    updated_at       timestamp with time zone not null default now(),
    completed_at     timestamp with time zone,
    primary key (activity_call_id, attempt_number),
    foreign key (activity_call_id) references underway.activity_call(id) on delete cascade
);

create index if not exists idx_activity_call_attempt_state
on underway.activity_call_attempt (state, started_at);

create index if not exists idx_activity_call_attempt_call
on underway.activity_call_attempt (activity_call_id, attempt_number desc);
