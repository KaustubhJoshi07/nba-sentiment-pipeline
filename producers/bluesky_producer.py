import boto3
import json
import time
import requests
from datetime import datetime
import sys
sys.path.append('..')
import config

kinesis = boto3.client('kinesis', region_name=config.AWS_REGION)

def get_bluesky_token():
    """
    Bluesky uses a simple username/password login to get
    an access token. This token expires after 2 hours —
    we'll handle refresh in the main loop.
    
    In production companies use OAuth2 with refresh tokens.
    For one game tonight, this simple approach is fine.
    """
    response = requests.post(
        "https://bsky.social/xrpc/com.atproto.server.createSession",
        json={
            "identifier": config.BLUESKY_HANDLE,
            "password": config.BLUESKY_PASSWORD
        }
    )
    
    if response.status_code != 200:
        raise Exception(f"Bluesky login failed: {response.text}")
    
    data = response.json()
    print(f"Bluesky login successful for {config.BLUESKY_HANDLE}")
    return data['accessJwt']

def search_posts(token, query, cursor=None):
    """
    Searches Bluesky for recent posts matching a query.
    
    Why cursor? Same concept as YouTube's page_token —
    it's Bluesky's way of paginating results so you
    only get NEW posts each time, not duplicates.
    """
    headers = {"Authorization": f"Bearer {token}"}
    
    params = {
        "q": query,
        "limit": 100,  # max per request
        "sort": "latest"  # most recent first
    }
    if cursor:
        params["cursor"] = cursor
    
    response = requests.get(
        "https://bsky.social/xrpc/app.bsky.feed.searchPosts",
        headers=headers,
        params=params
    )
    
    if response.status_code == 401:
        # Token expired — signal to refresh
        raise Exception("TOKEN_EXPIRED")
    
    if response.status_code != 200:
        raise Exception(f"Search failed: {response.text}")
    
    return response.json()

def push_to_kinesis(posts, query):
    """
    Same batching logic as YouTube producer.
    We tag each post with which search term found it —
    useful for analysis later. Did "Knicks" posts
    skew more positive than "Spurs" posts?
    """
    if not posts:
        return
    
    records = []
    for post in posts:
        # Extract the actual text from Bluesky's nested structure
        text = post.get('record', {}).get('text', '')
        if not text:
            continue
            
        record = {
            'source': 'bluesky',
            'text': text,
            'author': post.get('author', {}).get('handle', 'unknown'),
            'timestamp': datetime.utcnow().isoformat(),
            'game': 'NBA_Finals_2026',
            'search_term': query  # which keyword triggered this post
        }
        records.append({
            'Data': json.dumps(record),
            'PartitionKey': 'bluesky'
        })
    
    if not records:
        return
    
    for i in range(0, len(records), 500):
        chunk = records[i:i+500]
        response = kinesis.put_records(
            Records=chunk,
            StreamName=config.KINESIS_STREAM_NAME
        )
        failed = response.get('FailedRecordCount', 0)
        if failed > 0:
            print(f"Warning: {failed} Bluesky records failed")
        else:
            print(f"[Bluesky:{query}] Pushed {len(chunk)} posts to Kinesis")

def run():
    """
    Main loop — cycles through all search terms continuously.
    
    Why cycle through terms instead of running them in parallel?
    Bluesky's free API has rate limits. Cycling sequentially
    with a small sleep between each term stays safely within limits.
    For production you'd run parallel workers with rate limiting.
    """
    print("Starting Bluesky producer...")
    
    # Get initial token
    token = get_bluesky_token()
    token_refresh_time = time.time()
    
    while True:
        # Refresh token every 90 minutes before it expires
        if time.time() - token_refresh_time > 5400:
            print("Refreshing Bluesky token...")
            token = get_bluesky_token()
            token_refresh_time = time.time()
        
        for term in config.SEARCH_TERMS:
            try:
                print(f"Searching Bluesky for: {term}")
                result = search_posts(token, term)
                posts = result.get('posts', [])
                
                if posts:
                    push_to_kinesis(posts, term)
                    print(f"[{term}] Found {len(posts)} posts")
                else:
                    print(f"[{term}] No posts found")
                
                # Small delay between searches
                # Respectful API usage — don't hammer their servers
                time.sleep(1)
                
            except Exception as e:
                if "TOKEN_EXPIRED" in str(e):
                    print("Token expired mid-loop — refreshing...")
                    token = get_bluesky_token()
                    token_refresh_time = time.time()
                else:
                    print(f"Error searching {term}: {e}")
                    time.sleep(5)
        
        # Wait 30 seconds before cycling through terms again
        # During peak game moments you can reduce this to 10 seconds
        print(f"Cycle complete at {datetime.utcnow().strftime('%H:%M:%S')} — waiting 30s")
        time.sleep(30)

if __name__ == "__main__":
    run()