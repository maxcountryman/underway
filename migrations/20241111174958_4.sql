-- Force anything running this migration to use the right search path.
set local search_path to underway;

-- Rename the 'name' column in 'task_schedule' to 'task_queue_name' for consistency.
alter table underway.task_schedule
    rename column name to task_queue_name;

-- Update the primary key constraint to use the new column name.
alter table underway.task_schedule
    drop constraint task_schedule_pkey,
    add primary key (task_queue_name);
