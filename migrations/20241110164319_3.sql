-- Force anything running this migration to use the right search path.
set local search_path to underway;

alter table underway.task
add column if not exists heartbeat interval not null default interval '30 seconds';

alter table underway.task
add column if not exists last_heartbeat_at timestamp with time zone;
