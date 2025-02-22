import io
import pandas as pd
from airflow.models import Variable
from datetime import datetime, timedelta, timezone
from ..clients import get_minio_client
from airflow.models import Variable

CLIENT = get_minio_client()
BUCKET_NAME = Variable.get(
    "minio_bucket", deserialize_json=True
)

def file_exists_dump(filename: str):
    dump_filename = filename.replace('stage/', 'dump/')

    try:
        CLIENT.stat_object(BUCKET_NAME, dump_filename)
        return True
    except Exception:
        return False

def generate_IST_timestamp():
    current_timestamp = datetime.utcnow() + timedelta(hours = 5, minutes = 30)
    return current_timestamp.strftime('%Y-%m-%d %H:%M:%S')

def generate_timestamp(timestamp: str):
    timestamp_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    return timestamp_object.strftime("%Y%m%d%H%M%S")

def get_video_duration(youtube, video_id: str):
    video_response = youtube.videos().list(
        part = 'contentDetails',
        id = video_id
    ).execute()

    video_duration = video_response['items'][0]['contentDetails']['duration']
    return int(''.join(char for char in video_duration if char.isnumeric()))

def get_channel_handle(youtube, channel_id):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_id
    )
    response = request.execute()
    return response['items'][0]['snippet']['customUrl'][1:]

def get_videos(youtube, channel_id: str, max_videos = 7):
    videos = []
    channel_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None

    now = datetime.now(timezone.utc)
    seven_days = now - timedelta(days = 7)
    two_days = now - timedelta(days = 2)

    while True:
        playlist_items = youtube.playlistItems().list(
            part = "snippet",
            playlistId = uploads_playlist_id,
            maxResults = 50,
            pageToken = next_page_token
        ).execute()

        for item in playlist_items["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            video_title = item["snippet"]["title"]
            published_at = item["snippet"]["publishedAt"]

            video_release_date = datetime.strptime(
                    published_at, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)

            if video_release_date < seven_days:
                return videos

            if (
                    seven_days <= video_release_date <= two_days and
                    get_video_duration(youtube, video_id) > 300
            ):
                    videos.append({
                        "id": video_id,
                        "title": video_title,
                        "release": published_at
                    })

                    if len(videos) >= max_videos:
                        return videos

        next_page_token = playlist_items.get("nextPageToken")
        if not next_page_token:
            break

    return videos

def get_comments(youtube, video_id, video_title, video_release, channel_handle):
    comments = []
    next_page_token = None

    while True:
        request = youtube.commentThreads().list(
            part = "snippet",
            videoId = video_id,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append([
                comment['authorDisplayName'],
                comment['publishedAt'],
                comment['likeCount'],
                comment['textOriginal'],
                generate_IST_timestamp()
            ])

        next_page_token = response.get('nextPageToken')

        if not next_page_token:
            break

    filename = f"stage/{channel_handle}/{video_id}__{generate_timestamp(video_release)}.parquet"
    df = pd.DataFrame(comments, columns=[
        'author', 'updated_at', 'like_count', 'text', 'collected_at'
    ])

    if file_exists_dump(filename):
        print(f"Skipping file {filename} as it already exists in the dump/ folder.")
        return None

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)

    CLIENT.put_object(
        BUCKET_NAME, filename,
        io.BytesIO(parquet_buffer.getvalue()),
        length=len(parquet_buffer.getvalue())
    )

    return filename

def comments(youtube):
    channel_ids = Variable.get(
        "channel_ids", deserialize_json = True
    )

    processed_files = []
    skipped_files = []

    for channel_id in channel_ids:
        channel_handle = get_channel_handle(youtube, channel_id)
        videos = get_videos(youtube, channel_id)

        for video in videos:
            filename = get_comments(
                youtube,
                video['id'],
                video['title'],
                video['release'],
                channel_handle
            )

            if filename:
                processed_files.append(filename)
            else:
                skipped_files.append(filename)

    return {
        "processed": processed_files,
        "skipped": skipped_files
    }



