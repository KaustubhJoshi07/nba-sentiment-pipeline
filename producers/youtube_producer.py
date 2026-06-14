import boto3
import json
import time
from googleapiclient.discovery import build
from datetime import datetime
import sys
sys.path.insert(0, 'C:\\nba-sentiment-pipeline')
import config

# Why we build this client once outside the loop:
# Creating an API client is expensive — it makes network calls to Google.
# Building it once and reusing it saves time and API quota.
youtube = build('youtube', 'v3', developerKey=config.YOUTUBE_API_KEY)
kinesis = boto3.client('kinesis', region_name=config.AWS_REGION)

def get_live_chat_id(video_id):
    """
    Every YouTube live stream has a hidden 'liveChatId' behind the scenes.
    The video ID is what you see in the URL.
    The liveChatId is what the API needs to fetch comments.
    This function converts one to the other.
    """
    response = youtube.videos().list(
        part='liveStreamingDetails',
        id=video_id
    ).execute()
    
    if not response['items']:
        print(f"No video found with ID: {video_id}")
        return None
        
    details = response['items'][0].get('liveStreamingDetails', {})
    chat_id = details.get('activeLiveChatId')
    
    if not chat_id:
        print("No active live chat found — stream may not be live yet")
        return None
        
    return chat_id

def fetch_chat_messages(live_chat_id, page_token=None):
    """
    Fetches a batch of live chat messages.
    page_token is how YouTube handles pagination —
    each response gives you a token to get the NEXT batch.
    This is how you continuously pull new messages without
    getting duplicates.
    """
    params = {
        'liveChatId': live_chat_id,
        'part': 'snippet,authorDetails',
        'maxResults': 200
    }
    if page_token:
        params['pageToken'] = page_token
        
    return youtube.liveChatMessages().list(**params).execute()

def push_to_kinesis(messages, source_tag='youtube'):
    records = []
    for msg in messages:
        record = {
            'source': f'youtube_{source_tag}',  # tells you which stream
            'text': msg['snippet'].get('displayMessage') or msg['snippet'].get('textMessageDetails', {}).get('messageText', ''),
            'author': msg['authorDetails']['displayName'],
            'timestamp': datetime.utcnow().isoformat(),
            'game': 'NBA_Finals_2026'
        }
        records.append({
            'Data': json.dumps(record),
            'PartitionKey': source_tag  # different partition key per stream
        })
    
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        response = kinesis.put_records(
            Records=chunk,
            StreamName=config.KINESIS_STREAM_NAME
        )
        failed = response.get('FailedRecordCount', 0)
        if failed > 0:
            print(f"[{source_tag}] Warning: {failed} records failed")
        else:
            print(f"[{source_tag}] Pushed {len(chunk)} comments to Kinesis")

def run(video_ids):
    """
    Runs multiple YouTube streams simultaneously using threads.
    
    Why threads? Each stream needs its own polling loop running
    independently. If one stream goes offline or rate limits,
    the others keep running. This is the same pattern companies
    use for parallel data ingestion — each source gets its own
    worker thread.
    """
    import threading
    
    def run_single_stream(video_id):
        print(f"Starting stream for video: {video_id}")
        live_chat_id = get_live_chat_id(video_id)
        if not live_chat_id:
            print(f"Could not get live chat for {video_id} — skipping")
            return
            
        print(f"Live chat found for {video_id}: {live_chat_id}")
        page_token = None
        
        while True:
            try:
                response = fetch_chat_messages(live_chat_id, page_token)
                messages = response.get('items', [])
                
                # Tag each message with which stream it came from
                # So you can filter by stream in your dashboard later
                for msg in messages:
                    msg['_video_id'] = video_id
                
                if messages:
                    push_to_kinesis(messages, source_tag=video_id)
                    print(f"[{video_id}] Pushed {len(messages)} messages")
                
                poll_interval = response.get('pollingIntervalMillis', 5000) / 1000
                page_token = response.get('nextPageToken')
                time.sleep(poll_interval)
                
            except Exception as e:
                print(f"[{video_id}] Error: {e} — retrying in 10s")
                time.sleep(60)
    
    # Launch one thread per video stream
    threads = []
    for video_id in video_ids:
        t = threading.Thread(
            target=run_single_stream, 
            args=(video_id,),
            daemon=True  # Thread dies when main program exits
        )
        t.start()
        threads.append(t)
        print(f"Launched thread for stream: {video_id}")
    
    # Keep main program alive while threads run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping all streams...")

if __name__ == "__main__":
    # Add as many video IDs as you find tonight
    # Find them 30 min before tip-off from YouTube search
    VIDEO_IDS = [
        "a8PKHmysy9Y",
        "vQx7uaHZFlo",
        "dfBogB8WPUI",
        "9qQDaHGSqDU",
        "n1i6K_nW2y0",
        "xKJZT__xfyc",

    ]
    run(VIDEO_IDS)