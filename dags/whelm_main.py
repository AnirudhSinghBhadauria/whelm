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
from include.helpers.core.preprocess_comments import process

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
        youtube = get_youtube_client()
        processing_response = comments(youtube)

        return processing_response

    @task.pyspark(conn_id="whelm_core")
    def preprocess_comments(comment_files, spark: SparkSession, sc:SparkContext):
        curated_files = process(comment_files['processed'])

        return curated_files

    preprocess_comments(get_comments())

whelm()