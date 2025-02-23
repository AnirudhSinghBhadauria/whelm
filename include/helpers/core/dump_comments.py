from include.helpers.minio_read_write import (
    read_parquet_minio, write_parquet_minio
)

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get(
   "minio_bucket", deserialize_json=True
)

def berg_store(analyzed_files):
    dumped_files = []

    for file_path in analyzed_files:
        df = read_parquet_minio(CLIENT, BUCKET_NAME, file_path)

        dump_path = file_path.replace('processed/', 'dump/')
        write_parquet_minio(CLIENT, df, BUCKET_NAME, dump_path)

        dumped_files.append(dumped_path)
        CLIENT.remove_object(BUCKET_NAME, file_path)

    return dumped_files