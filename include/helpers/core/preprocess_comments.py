from airflow.models import Variable
from ..clients import get_minio_client

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get(
    "minio_bucket", deserialize_json=True
)

def process(youtube):
    pass