import numpy as np
import pandas as pd
from airflow.models import Variable
from include.helpers.clients import get_minio_client
from scipy.special import softmax
from transformers import AutoTokenizer
from transformers import AutoModelForSequenceClassification
from include.helpers.minio_read_write import (
    read_parquet_minio, write_parquet_minio
)

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get(
   "minio_bucket", deserialize_json=True
)
MODELNAME = "cardiffnlp/twitter-roberta-base-sentiment"

tokenizer = AutoTokenizer.from_pretrained(MODELNAME)
model = AutoModelForSequenceClassification.from_pretrained(MODELNAME)

def roberta(df):
    polarity = []
    for index, comment in df.iterrows():
        encoded_text = tokenizer(
            comment['text'], return_tensors='pt', max_length=512, truncation=True
        )
        output = model(**encoded_text)
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)
        df.at[index, 'negative'] = np.round(scores[0] * 100, 3)
        df.at[index, 'neutral'] = np.round(scores[1] * 100, 3)
        df.at[index, 'positive'] = np.round(scores[2] * 100, 3)

    for index, row in df.iterrows():
        max_index = np.argmax([row['negative'], row['neutral'], row['positive']])
        polarity.append(max_index)

    df['polarity'] = polarity
    df['polarity'] = df['polarity'].map({
        0: 'negative', 1: 'neutral', 2: 'positive'
    })

    return df

def analyze(preprocessed_files):
    analyzed_files = []

    for file_path in preprocessed_files:
        df = read_parquet_minio(CLIENT, BUCKET_NAME, file_path)

        analyzed_df = roberta(df)

        processed_path = file_path.replace('curated/', 'processed/')
        write_parquet_minio(CLIENT, analyzed_df, BUCKET_NAME, processed_path)

        analyzed_files.append(processed_path)
        CLIENT.remove_object(BUCKET_NAME, file_path)

    return analyzed_files