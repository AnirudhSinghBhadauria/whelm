from airflow.decorators import dag, task
from datetime import datetime, timedelta
from include.helpers.clients import get_youtube_client
from airflow.models import Variable
from pyspark import SparkContext
from pyspark.sql import SparkSession
from include.callbacks.status import (
    on_dag_failure, on_dag_success
)
from include.helpers.core.fetch_comments import comments

@dag(
    start_date = datetime(2025, 2, 15),
    schedule = "0 12 * * *",
    catchup = False,
    dagrun_timeout = timedelta(minutes = 5),
    on_success_callback = on_dag_success,
    on_failure_callback = on_dag_failure,
    tags = ['whelm: Youtube sentiment pipeline']
)
def whelm():
    @task.pyspark(conn_id = "whelm_core")
    def get_comments(spark: SparkSession, sc: SparkContext):
        DEVELOPER_KEY = Variable.get(
            "yt_developer_key", deserialize_json=False
        )

        youtube = get_youtube_client(DEVELOPER_KEY)

        processed_files = comments(youtube)


    get_comments()

whelm()