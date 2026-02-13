set local search_path to underway;

alter table underway.activity_call
    rename column workflow_id to workflow_run_id;

alter index if exists underway.idx_activity_call_workflow
    rename to idx_activity_call_workflow_run;

create or replace function underway.activity_call_change_notify()
returns trigger as $$
begin
    if (new.state = 'pending') then
        perform pg_notify(
            'activity_call_change',
            json_build_object(
                'task_queue_name', new.task_queue_name,
                'workflow_run_id', new.workflow_run_id,
                'step_index', new.step_index,
                'call_key', new.call_key
            )::text
        );
    end if;

    return new;
end;
$$ language plpgsql;
