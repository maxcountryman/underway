-- Force anything running this migration to use the right search path.
set local search_path to underway;

-- Create a temporary column to store serialized retry_policy data
alter table underway.task 
add column retry_policy_backup jsonb;

-- Save existing retry_policy data to backup column
update underway.task
set retry_policy_backup = jsonb_build_object(
    'max_attempts', (retry_policy).max_attempts,
    'initial_interval_ms', (retry_policy).initial_interval_ms,
    'max_interval_ms', (retry_policy).max_interval_ms,
    'backoff_coefficient', (retry_policy).backoff_coefficient
);

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

-- Migrate data from backup to new column
update underway.task
set retry_policy = row(
    (retry_policy_backup->>'max_attempts')::int,
    (retry_policy_backup->>'initial_interval_ms')::int,
    (retry_policy_backup->>'max_interval_ms')::int,
    (retry_policy_backup->>'backoff_coefficient')::float,
    0.5  -- default jitter_factor for existing records
)::underway.task_retry_policy
where retry_policy_backup is not null;

-- Drop the backup column
alter table underway.task 
drop column retry_policy_backup;
