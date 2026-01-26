-- Force anything running this migration to use the right search path.
set local search_path to underway;

create table if not exists underway.job_effect (
    job_id                   uuid not null,
    step_index               integer not null,
    effect_index             integer not null,
    compensation             jsonb,
    compensation_enqueued_at timestamp with time zone,
    compensated_at           timestamp with time zone,
    created_at               timestamp with time zone not null default now(),
    updated_at               timestamp with time zone not null default now(),
    primary key (job_id, effect_index)
);

create index if not exists idx_job_effect_job_id on underway.job_effect (job_id);
