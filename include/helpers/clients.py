from minio import Minio
from airflow.hooks.base import BaseHook
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def get_minio_client():
    minio = BaseHook.get_connection("crypto-minio")
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )

    return client

def get_youtube_client(developer_key: str):
    youtube = build("youtube", "v3", developerKey=developer_key)

    return youtube