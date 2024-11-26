-- Force anything running this migration to use the right search path.
set local search_path to underway;

alter table underway.task_attempt drop constraint task_attempt_task_id_task_queue_name_fkey;

alter table underway.task drop constraint task_pkey;
alter table underway.task add constraint task_pkey
primary key (task_queue_name, id);

alter table underway.task_attempt drop constraint task_attempt_pkey;
alter table underway.task_attempt add constraint task_attempt_pkey
primary key (task_queue_name, task_id, attempt_number);

alter table underway.task_attempt add constraint task_attempt_task_queue_name_task_id_fkey
foreign key (task_queue_name, task_id) references underway.task(task_queue_name, id) on delete cascade;
