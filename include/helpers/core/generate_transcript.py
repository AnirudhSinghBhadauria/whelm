from airflow.models import Variable
from include.helpers.minio_read_write import (
    read_parquet_minio, write_parquet_minio
)
from include.helpers.clients import get_minio_client
import pandas as pd
from io import BytesIO
from mistralai import Mistral

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get("minio_bucket", deserialize_json=True)
MISTRAL_KEY = Variable.get("mistral_key", deserialize_json=True)

def transcirpt_path(original_path):
    transformed_path = original_path.replace("processed", "transcript", 1)
    transformed_path = transformed_path.replace(".parquet", ".txt")

    return transformed_path

def generate_transcirpt(df, file_path):
    text_content = ""

    for index, row in df.iterrows():
        entry = f"Author: {row['author']}\n"
        entry += f"Text: {row['text']}\n"
        entry += f"Sentiment: {row['polarity']} (Positive: {row['positive']}%, Neutral: {row['neutral']}%, Negative: {row['negative']}%)\n"
        entry += f"Likes: {row['like_count']}\n"
        entry += f"Posted: {row['updated_at']}\n"
        entry += "---\n"
        text_content += entry

    mistral_client = Mistral(
        api_key = MISTRAL_KEY
    )

    completion = mistral_client.chat.complete(
        model = "mistral-large-latest",
        messages=[
            {
             "role": "system",
             "content": "You are a helpful assistant that analyzes YouTube comments. Focus ONLY on analyzing audience reactions and sentiment. Do not speculate about video content beyond what is directly mentioned in comments."
            },
            {
             "role": "user",
             "content": f"These are YouTube comments from a specific video. Please provide ONLY an analysis of audience reception and reactions. Focus on how viewers responded to the video, their sentiment, and any patterns in their feedback. Do NOT analyze the video content itself unless directly relevant to audience reactions.\n\n{text_content}"
            }
        ]
    )

    analysis_result = completion.choices[0].message.content
    transcript_filename = transcirpt_path(file_path)

    analysis_bytes = analysis_result.encode('utf-8')

    CLIENT.put_object(
        BUCKET_NAME,
        transcript_filename,
        BytesIO(analysis_bytes),
        len(analysis_bytes),
        content_type = "text/plain"
    )

    return file_path


def transcript(analyzed_files):
    transcripted_files = []

    for file_path in analyzed_files:
        df = read_parquet_minio(CLIENT, BUCKET_NAME, file_path)

        transcripted_file = generate_transcirpt(df, file_path)
        transcripted_files.append(transcripted_file)

    return transcripted_files