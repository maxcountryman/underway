-- Force anything running this migration to use the right search path.
set local search_path to underway;

alter table underway.task
    add column meta jsonb not null default '{}'::jsonb;
