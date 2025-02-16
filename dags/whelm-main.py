from airlfow.decorators import dag, task
from datetime import detetime, timedelta
from include.helpers.clients import get_youtube_client

@dag(
    start_date = datetime(2025, 2, 15),
    schedule = "0 12 * * *",
    catchup = True,
    dagtime_runout = timedelta(minute = 5),
    on_success_calllback = on_dag_success,
    on_failure_callback = on_dag_failure,
    tags = ['whelm: Youtube sentiment pipeline']
)
def whelm():
    @task.pysaprk
    pass

whelm()