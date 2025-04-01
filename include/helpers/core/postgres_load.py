from airflow.models import Variable
from include.helpers.clients import get_minio_client
import pandas as pd
from psycopg2.extras import execute_values
from include.helpers.core.cockroachdb_load import get_create_table_query
from include.helpers.minio_read_write import (
    read_parquet_minio, write_parquet_minio
)
import psycopg2
from contextlib import closing

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get("minio_bucket", deserialize_json=True)
POSTGRES_CONN_STRING = Variable.get("postgres_connection", deserialize_json=True)

def postgres(spark, analyzed_files):
    loaded_files = []

    for file_path in analyzed_files:
        schema = file_path.split('/')[-2]
        table_name = file_path.split('/')[-1].split('.')[0]

        df = read_parquet_minio(CLIENT, BUCKET_NAME, file_path)

        create_table_query = get_create_table_query(df, schema, table_name)

        with closing(psycopg2.connect(POSTGRES_CONN_STRING)) as conn:
            with closing(conn.cursor()) as cursor:
                cursor.execute(create_table_query)
                conn.commit()

        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]

        insert_query = f"""
            INSERT INTO {schema}."{table_name}" ({', '.join([f'"{col}"' for col in columns])})
            VALUES %s
            ON CONFLICT DO NOTHING
        """

        with closing(psycopg2.connect(POSTGRES_CONN_STRING)) as conn:
            with closing(conn.cursor()) as cursor:
                execute_values(
                    cursor,
                    insert_query,
                    values,
                    page_size=1000,
                    template=None
                )
                conn.commit()

        loaded_files.append(file_path)

    return loaded_files