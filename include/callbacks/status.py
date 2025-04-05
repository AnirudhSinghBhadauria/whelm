def on_dag_success(context):
    task_instances = context.get('task_instances', [])
    run_id = context.get('run_id', 'unknown')
    duration = context.get('duration', 'unknown')

    print(f"DAG SUCCESS: Run ID {run_id} completed successfully in {duration}")
    print(f"Total tasks executed: {len(task_instances)}")

    return 'Success'

def on_dag_failure(context):
    exception = context.get('exception', None)
    run_id = context.get('run_id', 'unknown')
    duration = context.get('duration', 'unknown')
    task_instances = context.get('task_instances', [])

    failed_tasks = []
    for ti in task_instances:
        if ti.state == 'failed':
            failed_tasks.append(ti.task_id)

    print(f"DAG FAILURE: Run ID {run_id} failed after {duration}")
    print(f"Failed tasks: {', '.join(failed_tasks) if failed_tasks else 'None'}")

    if exception:
        print(f"Exception: {str(exception)}")

    return 'Failure'