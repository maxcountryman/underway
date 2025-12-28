-- Force anything running this migration to use the right search path.
set local search_path to underway;

-- Save existing retry_policy data
alter table underway.task 
rename column retry_policy to retry_policy_old;

-- Drop and recreate the type with the new field
drop type if exists underway.task_retry_policy cascade;

create type underway.task_retry_policy as (
    max_attempts         int,
    initial_interval_ms  int,
    max_interval_ms      int,
    backoff_coefficient  float,
    jitter_factor        float
);

-- Add the new column with updated type
alter table underway.task
add column retry_policy underway.task_retry_policy not null 
default row(5, 1000, 60000, 2.0, 0.5)::underway.task_retry_policy;

-- Migrate data from old column to new column
update underway.task
set retry_policy = row(
    (retry_policy_old).max_attempts,
    (retry_policy_old).initial_interval_ms,
    (retry_policy_old).max_interval_ms,
    (retry_policy_old).backoff_coefficient,
    0.5  -- default jitter_factor for existing records
)::underway.task_retry_policy;

-- Drop the old column
alter table underway.task 
drop column retry_policy_old;
