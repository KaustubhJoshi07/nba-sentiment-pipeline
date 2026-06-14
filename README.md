# NBA Finals 2026 — Real-Time Fan Sentiment Pipeline

Built and deployed live during NBA Finals Game 5 (Knicks vs Spurs) on June 13, 2026.
The Knicks won their first championship since 1973. This pipeline caught every emotional swing.

![Architecture](architecture.png)

## Results
- 3,522 comments processed during the game
- 2 live data sources (YouTube + Bluesky)
- 5 AWS services
- Real-time sentiment spikes detected during Brunson's Q4 runs
- AI narrative generated every 5 minutes via AWS Bedrock

## Architecture
YouTube Live Chat + Bluesky → Kinesis Data Streams → Lambda → 
Comprehend → DynamoDB + S3 → Bedrock (Mistral 7B) → Streamlit

## Tech Stack
- **Ingest**: Python, YouTube Data API v3, Bluesky AT Protocol
- **Transport**: Amazon Kinesis Data Streams
- **Processing**: AWS Lambda + AWS Comprehend
- **Storage**: Amazon DynamoDB + Amazon S3
- **AI Narrative**: AWS Bedrock — Mistral 7B Instruct
- **Live Score**: ESPN Public API
- **Dashboard**: Streamlit

## Setup

1. Clone the repo
```bash
git clone https://github.com/yourusername/nba-sentiment-pipeline
cd nba-sentiment-pipeline
```

2. Install dependencies
```bash
pip install boto3 google-api-python-client streamlit pandas requests pytz
```

3. Configure AWS
```bash
aws configure
```

4. Copy config template
```bash
cp config.example.py config.py
# Fill in your API keys
```

5. Create AWS resources
- Kinesis Data Stream: `nba-sentiment-stream` (1 shard)
- DynamoDB table: `nba-sentiment` (partition: source, sort: timestamp)
- S3 bucket: `your-bucket-name`
- Lambda function with Kinesis trigger
- IAM role with Kinesis, DynamoDB, S3, Comprehend, Bedrock permissions

6. Run the pipeline
```bash
# Terminal 1 - Bluesky producer
python producers/bluesky_producer.py

# Terminal 2 - YouTube producer  
python producers/youtube_producer.py

# Terminal 3 - Dashboard
streamlit run dashboard/streamlit_app.py
```

## Project Structure 
