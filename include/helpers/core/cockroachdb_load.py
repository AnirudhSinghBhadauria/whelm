from airflow.models import Variable
from include.helpers.minio_read_write import (
    read_parquet_minio, write_parquet_minio
)
from include.helpers.clients import get_minio_client
import pandas as pd
from airflow.models import Variable
import psycopg2
from psycopg2.extras import execute_values

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get("minio_bucket", deserialize_json=True)
COCKROACHDB_CONNECTION = Variable.get("cockroach_connection", deserialize_json=True)

def get_create_table_query(df, schema, table_name):
    type_mapping = {
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'TEXT'
    }

    create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    columns = []
    for col_name, dtype in df.dtypes.items():
        pg_type = type_mapping.get(str(dtype), 'TEXT')
        columns.append(f'"{col_name}" {pg_type}')

    create_table = f"""
       {create_schema}
       CREATE TABLE IF NOT EXISTS {schema}."{table_name}" (
           {', '.join(columns)}
       );
       """

    return create_table

def cockroachdb(spark, processed_files):
    loaded_files = []
    conn = psycopg2.connect(
        COCKROACHDB_CONNECTION['url'],
        sslmode='require'
    )
    cursor = conn.cursor()

    for file_path in processed_files:
        schema = file_path.split('/')[-2]
        table_name = file_path.split('/')[-1].split('.')[0]

        df = read_parquet_minio(CLIENT, BUCKET_NAME, file_path)

        create_table_query = get_create_table_query(df, schema, table_name)
        cursor.execute(create_table_query)

        columns = df.columns.tolist()
        values = [tuple(row) for row in df.values]

        insert_query = f"""
            INSERT INTO {schema}."{table_name}" ({', '.join(columns)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        execute_values(cursor, insert_query, values, page_size=1000)

        conn.commit()

        print(f"Successfully loaded {file_path} to {schema}.{table_name}")
        loaded_files.append(file_path)

    cursor.close()
    conn.close()

    return loaded_files




