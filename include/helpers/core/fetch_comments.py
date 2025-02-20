from airflow.models import Variable
from datetime import datetime, timedelta, timezone

def get_video_duration(youtube, video_id: str):
    video_response = youtube.videos().list(
        part = 'contentDetails',
        id = video_id
    ).execute()
    video_duration = video_response['items'][0]['contentDetails']['duration']

    return int(''.join(char for char in video_duration if char.isnumeric()))

def get_channel_handle(youtube, channel_id):

    print(f"\n\n{channel_id}\n\n")
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_id
    )
    response = request.execute()
    return response

def get_videos(youtube, channel_id: str):
    videos = []
    channel_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()

    # print(channel_response)

    uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None

    now = datetime.now(timezone.utc)
    seven_days = now - timedelta(days=7)

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

            # print(published_at)

            video_release_date = datetime.strptime(
                    published_at, "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)

            if (video_release_date >= seven_days and
                get_video_duration(youtube, video_id) > 300):
                    videos.append({
                        "id": video_id,
                        "title": video_title,
                        "release": published_at
                    })

    return videos

def get_comments():
    pass

def comments(youtube):
    channel_ids = Variable.get(
        "channel_ids", deserialize_json = True
    )

    processed_files = []
    skipped_files = []

    for channel_id in channel_ids:
        # print(f"\n\n{channel_id}\n\n")
        channel_handle = get_channel_handle(youtube, channel_id)
        videos = get_videos(youtube, channel_id)

        print(channel_handle)
        print(videos)


        # for video in videos:
        #     filename = get_comments()

