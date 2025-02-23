import pandas as pd
import io

def read_parquet_minio(client, bucket_name, file_path):
   data = client.get_object(bucket_name, file_path).read()
   return pd.read_parquet(io.BytesIO(data))

def write_parquet_minio(client, df, bucket_name, output_path):
   buffer = io.BytesIO()
   df.to_parquet(buffer)
   buffer.seek(0)

   client.put_object(
      bucket_name=bucket_name,
      object_name=output_path,
      data=buffer,
      length=buffer.getbuffer().nbytes
   )
