from airflow.models import Variable
from include.helpers.minio_read_write import (
    read_parquet_minio, write_parquet_minio
)
from include.helpers.clients import get_minio_client
import pandas as pd
from io import BytesIO
from mistralai import Mistral
import math

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get("minio_bucket", deserialize_json=True)
MISTRAL_KEY = Variable.get("mistral_key", deserialize_json=True)
MAX_TOKENS_PER_BATCH = 40000
CHARS_PER_TOKEN = 3.0


def transcript_path(original_path):
    transformed_path = original_path.replace("processed", "transcript", 1)
    transformed_path = transformed_path.replace(".parquet", ".txt")
    return transformed_path


def generate_transcript(df, file_path):
    if len(df) > 10000:
        print(f"Large dataset detected ({len(df)} rows), sampling to 10000 rows")
        df = df.sample(n=10000, random_state=42)

    entries = []
    for index, row in df.iterrows():
        entry = f"Author: {row['author']}\n"
        entry += f"Text: {row['text']}\n"
        entry += f"Sentiment: {row['polarity']} (Positive: {row['positive']}%, Neutral: {row['neutral']}%, Negative: {row['negative']}%)\n"
        entry += f"Likes: {row['like_count']}\n"
        entry += f"Posted: {row['updated_at']}\n"
        entry += "---\n"
        entries.append(entry)

    estimated_chars = sum(len(entry) for entry in entries)
    estimated_tokens = math.ceil(estimated_chars / CHARS_PER_TOKEN)

    min_batches = 1
    if estimated_tokens > 100000:
        min_batches = 4
    elif estimated_tokens > 50000:
        min_batches = 2

    num_batches = max(min_batches, math.ceil(estimated_tokens / MAX_TOKENS_PER_BATCH))

    print(f"Processing file with estimated {estimated_tokens} tokens in {num_batches} batches")

    mistral_client = Mistral(
        api_key=MISTRAL_KEY
    )

    batch_analyses = []

    batch_size = math.ceil(len(entries) / num_batches)
    batches = [entries[i:i + batch_size] for i in range(0, len(entries), batch_size)]

    for i, batch in enumerate(batches):
        batch_text = "".join(batch)

        if not batch_text.strip():
            continue

        batch_chars = len(batch_text)
        batch_tokens = math.ceil(batch_chars / CHARS_PER_TOKEN)

        if batch_tokens > 100000:
            print(f"Batch {i + 1} has {batch_tokens} tokens, skipping as it's too large")
            continue

        batch_prompt = f"These are YouTube comments from a specific video. Please provide a brief analysis of the audience reception and reactions. Keep your analysis concise."

        try:
            print(f"Processing batch {i + 1}/{len(batches)}: ~{batch_tokens} tokens")

            completion = mistral_client.chat.complete(
                model="mistral-large-latest",
                messages=[
                    {
                        "role": "system",
                        "content": "Analyze YouTube comments concisely without using any headers or section titles."
                    },
                    {
                        "role": "user",
                        "content": f"{batch_prompt}\n\n{batch_text}"
                    }
                ]
            )
            batch_analysis = completion.choices[0].message.content
            batch_analyses.append(batch_analysis)
        except Exception as e:
            error_msg = str(e)
            print(f"Error processing batch {i + 1}: {error_msg}")

            if "too large for model" in error_msg and len(batch) > 10:
                try:
                    half_size = len(batch) // 2
                    smaller_batch = batch[:half_size]
                    smaller_text = "".join(smaller_batch)

                    print(f"Retrying with smaller half of batch {i + 1}")
                    completion = mistral_client.chat.complete(
                        model="mistral-large-latest",
                        messages=[
                            {
                                "role": "system",
                                "content": "Analyze YouTube comments concisely without using any headers or section titles."
                            },
                            {
                                "role": "user",
                                "content": f"These are some YouTube comments. Please provide a brief analysis of the audience reactions.\n\n{smaller_text}"
                            }
                        ]
                    )
                    partial_analysis = completion.choices[0].message.content
                    batch_analyses.append(partial_analysis)
                except Exception as inner_e:
                    print(f"Error with smaller batch {i + 1}: {str(inner_e)}")

    combined_analysis = ""

    if batch_analyses:
        combined_analysis = "\n\n".join(batch_analyses)

    if batch_analyses:
        try:
            summary_prompt = "Summarize these YouTube comment analyses concisely without using any headers."

            final_summary = mistral_client.chat.complete(
                model="mistral-large-latest",
                messages=[
                    {
                        "role": "system",
                        "content": "Summarize analyses briefly without using any headers or section titles."
                    },
                    {
                        "role": "user",
                        "content": f"{summary_prompt}\n\n{combined_analysis}"
                    }
                ]
            )
            combined_analysis += "\n\n" + final_summary.choices[0].message.content
        except Exception as e:
            print(f"Error creating final summary: {str(e)}")

    transcript_filename = transcript_path(file_path)
    analysis_bytes = combined_analysis.encode('utf-8')

    CLIENT.put_object(
        BUCKET_NAME,
        transcript_filename,
        BytesIO(analysis_bytes),
        len(analysis_bytes),
        content_type="text/plain"
    )

    return file_path


def transcript(analyzed_files):
    transcripted_files = []

    for file_path in analyzed_files:
        df = read_parquet_minio(CLIENT, BUCKET_NAME, file_path)
        transcripted_file = generate_transcript(df, file_path)
        transcripted_files.append(transcripted_file)

    return transcripted_files