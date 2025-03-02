from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.models import Variable
from pyspark import SparkContext
from pyspark.sql import SparkSession
from include.helpers.core.fetch_comments import comments
from include.helpers.core.preprocess_comments import process
from include.helpers.core.analyze_comments import analyze
from include.helpers.core.dump_comments import berg_store
from include.helpers.core.generate_transcript import transcript
from include.helpers.core.cockroachdb_load import cockroachdb
from include.helpers.clients import get_youtube_client
from include.callbacks.status import (
    on_dag_failure, on_dag_success
)

@dag(
    start_date = datetime(2025, 2, 15),
    schedule = "0 12 * * *",
    catchup = False,
    dagrun_timeout = timedelta(minutes = 30),
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

    @task.pyspark(conn_id="whelm_core")
    def analyze_comments(preprocessed_files, spark: SparkSession, sc: SparkContext):
        analyzed_files = analyze(preprocessed_files)

        return analyzed_files

    @task.pyspark(conn_id="whelm_core")
    def generate_transcript(analyzed_files, spark: SparkSession, sc: SparkContext):
        transcripted_files = transcript(analyzed_files)

        return  transcripted_files

    @task.pyspark(conn_id = "whelm_core")
    def load_cockroachdb(analyzed_files, spark: SparkSession, sc: SparkContext):
        loaded_files = cockroachdb(spark, analyzed_files)

        return loaded_files

    @task
    def dump(processed_files):
        dumped_files = berg_store(processed_files)

        print(
            f"Following files are analyzed successfully: {dumped_files}"
        )

    load_cockroachdb(
        generate_transcript(
            analyze_comments(
                preprocess_comments(
                    get_comments()
                )
            )
        )
    )

whelm()