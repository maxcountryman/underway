-- function to notify about task changes
create or replace function underway.task_change_notify()
returns trigger as $$
begin
  if (new.state = 'pending') then
    perform pg_notify('task_change', json_build_object(
      'task_queue_name', new.task_queue_name
    )::text);
  end if;

  return new;
end;
$$ language plpgsql;

-- trigger that calls the function after task changes
create trigger task_changed
after insert or update on underway.task
for each row
execute procedure underway.task_change_notify();
