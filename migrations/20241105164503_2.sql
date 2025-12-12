-- Force anything running this migration to use the right search path.
set local search_path to underway;

-- Remove attempt-specific columns from `task`
alter table underway.task
drop column if exists retry_count,
drop column if exists max_attempts,
drop column if exists initial_interval_ms,
drop column if exists max_interval_ms,
drop column if exists backoff_coefficient,
drop column if exists error_message,
drop column if exists started_at,
drop column if exists succeeded_at,
drop column if exists last_failed_at;

-- Define retry policies as their own type
create type underway.task_retry_policy as (
    max_attempts         int,
    initial_interval_ms  int,
    max_interval_ms      int,
    backoff_coefficient  float,
    jitter_factor        float
);

alter table underway.task
    add column if not exists retry_policy underway.task_retry_policy not null 
    default row(5, 1000, 60000, 2.0, 0.5)::underway.task_retry_policy;

alter table underway.task
add column if not exists completed_at timestamp with time zone;

alter table underway.task
add column if not exists last_attempt_at timestamp with time zone;

create table underway.task_attempt (
    task_id         uuid not null,
    task_queue_name text not null,
    attempt_number  integer not null,

    -- Task state.
    state           underway.task_state not null default 'in_progress',

    -- Error metatdata.
    error_message   text,

    started_at      timestamp with time zone not null default now(),
    updated_at      timestamp with time zone not null default now(),
    completed_at    timestamp with time zone,

    primary key (task_id, task_queue_name, attempt_number),
    foreign key (task_id, task_queue_name) references underway.task(id, task_queue_name) on delete cascade
);
