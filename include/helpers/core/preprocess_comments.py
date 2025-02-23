import re, emoji
import pandas as pd
from airflow.models import Variable
from include.helpers.clients import get_minio_client
from include.helpers.minio_read_write import (
    write_parquet_minio, read_parquet_minio
)

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get(
   "minio_bucket", deserialize_json=True
)

def preprocess_yt_comments(text):
   if not isinstance(text, str):
       return ''

   text = text.lower()
   text = emoji.demojize(text)
   text = re.sub(r'https?://\S+|www\.\S+', '', text, flags=re.MULTILINE)
   text = re.sub(r'@\w+', '', text)
   text = re.sub(r'<[^>]+>', '', text)
   text = re.sub(r'(.)\1{2,}', r'\1\1', text)
   text = re.sub(r'[^a-z0-9\s.,!?\U0001F300-\U0001F9FF\U0001FA70-\U0001FAFF]', '', text)
   text = re.sub(r'\s+', ' ', text).strip()
   text = emoji.emojize(text)

   return text

def clean_comments(df):
   df = df.drop_duplicates()
   df = df.dropna()
   df['text'] = df['text'].apply(preprocess_yt_comments)
   df = df[df['text'] != '']
   df = df.dropna(subset=['text'])
   return df

def process(comment_files):
   curated_files = []

   for file_path in comment_files:
       df = read_parquet_minio(CLIENT, BUCKET_NAME, file_path)
       processed_df = clean_comments(df)

       curated_path = file_path.replace('stage/', 'curated/')
       write_parquet_minio(CLIENT, processed_df, BUCKET_NAME, curated_path)

       curated_files.append(curated_path)
       CLIENT.remove_object(BUCKET_NAME, file_path)

   return curated_files