from minio import Minio
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def get_minio_client():
    minio = BaseHook.get_connection("whelm_minio")
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )

    return client

def get_youtube_client():
    DEVELOPER_KEY = Variable.get(
        "yt_developer_key", deserialize_json=True
    )
    youtube = build("youtube", "v3", developerKey=DEVELOPER_KEY)

    return youtube