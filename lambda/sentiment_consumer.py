import boto3
import json
import time
from datetime import datetime, timedelta
import os

# Why os.environ instead of importing config?
# Lambda runs on AWS servers, not your laptop.
# It can't import your local config.py file.
# Environment variables are how you pass configuration
# to Lambda functions — standard practice everywhere.
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE_NAME', 'nba-sentiment')
S3_BUCKET = os.environ.get('S3_BUCKET_NAME', 'nba-sentiment-raw-kj')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Initialize AWS clients outside the handler function
# Why? Lambda reuses the same container for multiple invocations.
# Initializing clients outside means they're created once
# and reused — faster execution, lower cost.
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)
comprehend = boto3.client('comprehend', region_name=AWS_REGION)

table = dynamodb.Table(DYNAMODB_TABLE)

def analyze_sentiment(texts):
    """
    Comprehend accepts up to 25 texts per batch call.
    We batch to minimize API calls — same cost principle
    as Kinesis batching in the producers.
    
    Why detect_sentiment_batch vs calling one at a time?
    25 individual calls = 25 API requests = slower and costlier.
    1 batch call with 25 texts = 1 API request = faster and cheaper.
    This is called batching — fundamental optimization in every
    real data pipeline.
    """
    results = []
    
    # Split into chunks of 25 (Comprehend's max batch size)
    for i in range(0, len(texts), 25):
        chunk = texts[i:i+25]
        
        try:
            response = comprehend.batch_detect_sentiment(
                TextList=chunk,
                LanguageCode='en'
            )
            
            for result in response['ResultList']:
                sentiment = result['Sentiment']  # POSITIVE/NEGATIVE/NEUTRAL/MIXED
                scores = result['SentimentScore']
                
                results.append({
                    'sentiment': sentiment,
                    'positive_score': round(scores['Positive'], 4),
                    'negative_score': round(scores['Negative'], 4),
                    'neutral_score': round(scores['Neutral'], 4),
                    'mixed_score': round(scores['Mixed'], 4)
                })
                
        except Exception as e:
            print(f"Comprehend error on batch: {e}")
            # If Comprehend fails on a batch, mark as NEUTRAL
            # rather than dropping the records entirely
            for _ in chunk:
                results.append({
                    'sentiment': 'NEUTRAL',
                    'positive_score': 0,
                    'negative_score': 0,
                    'neutral_score': 1,
                    'mixed_score': 0
                })
    
    return results

def filter_spam(records):
    """
    Removes bot spam and promotional comments.
    These are common on YouTube live streams —
    bots promoting illegal streaming sites flood
    the chat especially during big games.
    Including them would corrupt your sentiment scores.
    Real platforms like YouTube and Twitch run
    much more sophisticated versions of this filter.
    """
    spam_keywords = [
        'nbatvs.com', 'nbalp.com', 'nbafinal.com',
        'watch live', 'free stream', 'live stream',
        'bit.ly', 'tinyurl', '.com👇', 'nbatvs',
        'nbalp', 'stream free', 'watch here'
    ]
    
    filtered = []
    for record in records:
        text = record.get('text', '').lower()
        
        # Skip if contains spam keywords
        if any(keyword in text for keyword in spam_keywords):
            continue
            
        # Skip if too short — "lol" "wow" adds no sentiment value
        if len(text) < 10:
            continue
            
        # Skip if more than 3 @ mentions — likely a bot
        if text.count('@') > 3:
            continue
            
        filtered.append(record)
    
    removed = len(records) - len(filtered)
    if removed > 0:
        print(f"Filtered {removed} spam records")
    
    return filtered

def calculate_momentum(source, current_window):
    """
    Momentum = how fast sentiment is changing.
    We compare current 5-minute window vs previous 5-minute window.
    
    This is the metric that makes your dashboard interesting —
    raw sentiment tells you current mood,
    momentum tells you if things are getting better or worse.
    
    Real trading platforms use this exact concept for price momentum.
    Sports analytics companies use it for fan engagement scoring.
    """
    try:
        now = datetime.utcnow()
        prev_window = (now - timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M')
        
        response = table.get_item(
            Key={
                'source': source,
                'timestamp': prev_window
            }
        )
        
        if 'Item' not in response:
            return 0  # No previous window to compare
        
        prev_sentiment = float(response['Item'].get('avg_positive', 0))
        momentum = round(current_window - prev_sentiment, 4)
        return momentum
        
    except Exception as e:
        print(f"Momentum calculation error: {e}")
        return 0

def save_to_dynamodb(source, window_timestamp, records, sentiment_results):
    """
    We store AGGREGATED data in DynamoDB, not individual comments.
    Why? During peak game moments you might get 500 comments
    per minute. Storing each one individually would make
    DynamoDB expensive and your dashboard queries slow.
    
    Instead we compute averages per 5-minute window per source.
    Your dashboard then reads clean pre-aggregated data —
    fast queries, low cost, clear visualization.
    
    This is called pre-aggregation — a core technique in
    every analytics system. Google Analytics does this.
    Mixpanel does this. Every dashboard tool does this.
    """
    if not sentiment_results:
        return
    
    # Calculate averages for this window
    avg_positive = sum(r['positive_score'] for r in sentiment_results) / len(sentiment_results)
    avg_negative = sum(r['negative_score'] for r in sentiment_results) / len(sentiment_results)
    
    # Count sentiment distribution
    sentiment_counts = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0, 'MIXED': 0}
    for r in sentiment_results:
        sentiment_counts[r['sentiment']] = sentiment_counts.get(r['sentiment'], 0) + 1
    
    # Calculate momentum
    momentum = calculate_momentum(source, avg_positive)
    
    # TTL = 24 hours from now
    # DynamoDB will automatically delete this record tomorrow
    ttl = int(time.time()) + 86400
    
    # Store 5 sample comments per window
# Why? Dashboard needs real comments to display
# what fans are actually saying — not just numbers
    sample_comments = []
    for i, record in enumerate(records[:5]):
        if i < len(sentiment_results):
            sample_comments.append({
                'text': record.get('text', '')[:100],
                'sentiment': sentiment_results[i]['sentiment'],
                'author': record.get('author', 'unknown')
            })

    item = {
        'source': source,
        'timestamp': window_timestamp,
        'avg_positive': str(round(avg_positive, 4)),
        'avg_negative': str(round(avg_negative, 4)),
        'total_comments': len(sentiment_results),
        'sentiment_counts': sentiment_counts,
        'momentum': str(momentum),
        'game': 'NBA_Finals_2026',
        'expiry': ttl,
        'sample_comments': sample_comments  # new
    }
    
    table.put_item(Item=item)
    print(f"Saved to DynamoDB: {source} at {window_timestamp} — {len(sentiment_results)} comments, avg_positive={round(avg_positive,4)}, momentum={momentum}")

def save_raw_to_s3(records, window_timestamp):
    """
    Raw comments go to S3 exactly as received — no processing.
    Why keep raw data if we already have aggregates in DynamoDB?
    
    Because aggregates are lossy — once you average 500 comments
    into one number, you can't get the original comments back.
    Raw data in S3 lets you:
    1. Reprocess with better algorithms later
    2. Do deeper analysis after the game
    3. Train ML models on real game sentiment data
    
    Storage cost for one game: less than $0.01.
    Value of having raw data: priceless for a portfolio project.
    """
    if not records:
        return
    
    # One S3 file per window per source
    key = f"raw/{window_timestamp[:10]}/{window_timestamp}.json"
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(records, indent=2),
        ContentType='application/json'
    )
    print(f"Saved {len(records)} raw records to S3: {key}")

def deduplicate_records(records):
    """
    Removes duplicate comments based on identical text
    within the same processing batch.
    
    Why text-based deduplication instead of user-based?
    We have no reliable cross-stream user identifier.
    But identical text from different sources within
    seconds of each other is almost certainly spam,
    bots, or copy-paste — not genuine sentiment.
    
    Twitter, Reddit, and YouTube all use this same
    approach internally for spam filtering.
    """
    seen_texts = set()
    unique_records = []
    
    for record in records:
        # Normalize text — lowercase, strip whitespace
        # So "LETS GO KNICKS" and "lets go knicks" 
        # count as the same comment
        normalized = record.get('text', '').lower().strip()
        
        # Skip empty comments
        if not normalized:
            continue
        
        # Skip if we've seen this exact text already
        if normalized in seen_texts:
            continue
            
        seen_texts.add(normalized)
        unique_records.append(record)
    
    duplicates_removed = len(records) - len(unique_records)
    if duplicates_removed > 0:
        print(f"Removed {duplicates_removed} duplicate comments")
    
    return unique_records

def handler(event, context):
    """
    This is the entry point Lambda calls automatically
    when new Kinesis records arrive.
    
    'event' contains batches of Kinesis records.
    'context' contains Lambda metadata (timeout, memory etc).
    
    Why is it called 'handler'? Convention — AWS looks for
    this function name by default when triggering Lambda.
    You can name it anything but handler is standard.
    """
    print(f"Lambda triggered — processing {len(event['Records'])} Kinesis records")
    
    # Group records by source
    # Why group? We want separate sentiment scores for
    # YouTube vs Bluesky — they have different audiences
    # and different writing styles
    records_by_source = {}
    
    for kinesis_record in event['Records']:
        # Kinesis records are base64 encoded — decode them
        import base64
        raw_data = base64.b64decode(kinesis_record['kinesis']['data'])
        record = json.loads(raw_data)
        
        source = record.get('source', 'unknown')
        if source not in records_by_source:
            records_by_source[source] = []
        records_by_source[source].append(record)
    
    # Current 5-minute window timestamp
    # Why round to 5 minutes? All comments in the same
    # 5-minute window get aggregated together in DynamoDB
    now = datetime.utcnow()
    window_minutes = (now.minute // 5) * 5
    window_timestamp = now.strftime(f'%Y-%m-%dT%H:{window_minutes:02d}')
    
    # Process each source independently
    for source, records in records_by_source.items():
        print(f"Processing {len(records)} records from {source}")
        
        # Extract text for sentiment analysis
        unique_records = deduplicate_records(records)
        unique_records = filter_spam(unique_records)
        texts = [r.get('text', '') for r in unique_records if r.get('text', '').strip()]
        
        if not texts:
            print(f"No text found in {source} records — skipping")
            continue
        
        # Analyze sentiment in batches
        sentiment_results = analyze_sentiment(texts)
        
        # Save aggregated results to DynamoDB
        save_to_dynamodb(source, window_timestamp, records, sentiment_results)
        
        # Save raw records to S3
        save_raw_to_s3(records, window_timestamp)
    
    print("Lambda execution complete")
    return {'statusCode': 200, 'body': 'Success'}