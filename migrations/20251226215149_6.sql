-- Force anything running this migration to use the right search path.
set local search_path to underway;

alter table underway.task
add column if not exists lease_token uuid;

alter table underway.task
add column if not exists lease_expires_at timestamp with time zone;

alter table underway.task_attempt
add column if not exists lease_token uuid;
