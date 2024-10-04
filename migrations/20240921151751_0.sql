create schema if not exists underway;

create table underway.task_queue (
    name       text not null,
    dlq_name   text,
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null default now(),
    primary key (name),
    foreign key (dlq_name) references underway.task_queue(name)
);

create table underway.task_schedule (
    name       text references underway.task_queue on delete cascade,
    schedule   text not null,
    timezone   text not null,
    input      jsonb not null,
    created_at timestamp with time zone not null default now(),
    updated_at timestamp with time zone not null default now(),
    primary key (name)
);

create type underway.task_state as enum (
    'pending',
    'in_progress',
    'succeeded',
    'cancelled',
    'failed'
);

create table underway.task (
    id                  uuid not null,
    task_queue_name     text not null,
    input               jsonb not null,
    state               underway.task_state not null default 'pending',
    priority            integer not null default 0,
    concurrency_key     text,
    error_message       text,
    retry_count         integer not null default 0,
    max_attempts        integer not null default 5,
    initial_interval_ms integer not null default 1000,
    max_interval_ms     integer not null default 60000,
    backoff_coefficient real not null default 2.0,
    timeout             interval not null default interval '15 minutes',
    ttl                 interval not null default interval '14 days',
    delay               interval not null default interval '0',
    created_at          timestamp with time zone not null default now(),
    updated_at          timestamp with time zone not null default now(),
    started_at          timestamp with time zone,
    succeeded_at        timestamp with time zone,
    last_failed_at      timestamp with time zone,
    primary key (id, task_queue_name),
    foreign key (task_queue_name) references underway.task_queue(name)
);

create unique index idx_task_concurrency_key_unique
on underway.task (task_queue_name, concurrency_key)
where concurrency_key is not null and state in ('pending', 'in_progress');
