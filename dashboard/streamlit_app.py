import streamlit as st
import boto3
import json
import requests
import time
from datetime import datetime, timedelta
import pandas as pd
from datetime import timezone
import pytz
cst = pytz.timezone('America/Chicago')
st.set_page_config(
    page_title="NBA Finals Sentiment Dashboard",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Why session state?
# Streamlit reruns the entire script every refresh.
# Session state persists variables across reruns —
# without it your AI narrative would disappear every 30 seconds.
if 'last_bedrock_update' not in st.session_state:
    st.session_state.last_bedrock_update = None
if 'ai_narrative' not in st.session_state:
    st.session_state.ai_narrative = "Waiting for game data..."
if 'bedrock_history' not in st.session_state:
    st.session_state.bedrock_history = []
if 'total_comments_all_time' not in st.session_state:
    st.session_state.total_comments_all_time = 0
if 'last_comment_count' not in st.session_state:
    st.session_state.last_comment_count = 0
if 'peak_comments_per_window' not in st.session_state:
    st.session_state.peak_comments_per_window = 0

# AWS clients
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('nba-sentiment')
bedrock = boto3.client('bedrock-runtime', region_name='us-east-1')

def get_live_score():
    """
    ESPN public API — no key needed.
    Pulls live score, quarter, clock directly.
    """
    try:
        url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
        response = requests.get(url, timeout=5)
        data = response.json()
        
        for event in data.get('events', []):
            name = event.get('name', '')
            if 'Knicks' in name or 'Spurs' in name:
                competition = event['competitions'][0]
                competitors = competition['competitors']
                status = competition['status']['type']['description']
                period = competition['status'].get('period', 0)
                clock = competition['status'].get('displayClock', '')
                
                home = next(t for t in competitors if t['homeAway'] == 'home')
                away = next(t for t in competitors if t['homeAway'] == 'away')
                
                return {
                    'home_team': home['team']['displayName'],
                    'home_score': home.get('score', '0'),
                    'away_team': away['team']['displayName'],
                    'away_score': away.get('score', '0'),
                    'status': status,
                    'quarter': period,
                    'clock': clock
                }
        return None
    except Exception as e:
        return None

def get_sentiment_data():
    """
    Pulls last 12 windows (1 hour) from DynamoDB.
    Why scan with filter instead of query?
    We want data from ALL sources in one call.
    In production you'd use a GSI for this —
    but for one game, a filtered scan is fine.
    """
    try:
        cst = pytz.timezone('America/Chicago')
        now_cst = datetime.now(cst)
        one_hour_ago = (now_cst - timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M')
        
        response = table.scan(
            FilterExpression='#ts >= :cutoff',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={':cutoff': one_hour_ago}
        )
        
        items = response.get('Items', [])
        return items
    except Exception as e:
        st.error(f"DynamoDB error: {e}")
        return []

def get_ai_narrative(sentiment_data, score_data):
    """
    Calls Bedrock every 5 minutes only.
    Why throttle? Bedrock costs per token.
    Calling every 30 seconds = 120 calls/hour.
    Calling every 5 minutes = 12 calls/hour.
    10x cheaper with no meaningful quality loss.
    """
    now = datetime.now(cst)
    
    # Check if 5 minutes have passed since last call
    if st.session_state.last_bedrock_update:
        last_update = st.session_state.last_bedrock_update
        if last_update.tzinfo is None:
            last_update = last_update.replace(tzinfo=pytz.UTC)
        elapsed = (now - last_update).seconds
        if elapsed < 300:
            return st.session_state.ai_narrative
    
    if not sentiment_data:
        return "Waiting for sentiment data to accumulate..."
    
    # Build context for Bedrock
    recent = sentiment_data[-3:] if len(sentiment_data) >= 3 else sentiment_data
    
    score_context = ""
    if score_data:
        score_context = f"""
        Current score: {score_data['away_team']} {score_data['away_score']} vs {score_data['home_team']} {score_data['home_score']}
        Quarter: {score_data['quarter']} | Clock: {score_data['clock']}
        """
    
    sentiment_context = "\n".join([
        f"- {item['source']} at {item['timestamp']}: {item['avg_positive']} positive, {item['total_comments']} comments, momentum {item['momentum']}"
        for item in recent
    ])
    
    prompt = f"""<s>[INST] You are a live sports analyst covering NBA Finals Game 5 (Knicks vs Spurs) on social media.

    {score_context}

    Fan sentiment data from last 15 minutes:
    {sentiment_context}

    Give me a PUNCHY 3-bullet analysis. Each bullet max 1 sentence. Format exactly like this:

    🔥 ENERGY: [which team's fans are louder and why]
    📉 MOOD SHIFT: [biggest sentiment change and what caused it]
    ⚡ MOMENTUM: [is crowd energy rising or falling right now]

    Be specific. Use basketball language. Sound like a real analyst not a data report. [/INST]"""

    try:
        response = bedrock.invoke_model(
            modelId='mistral.mistral-7b-instruct-v0:2',
            body=json.dumps({
                "prompt": f"<s>[INST] {prompt} [/INST]",
                "max_tokens": 300,
                "temperature": 0.7,
                "top_p": 0.9
            })
        )
        
        result = json.loads(response['body'].read())
        narrative = result['outputs'][0]['text']
        
        # Update session state
        st.session_state.ai_narrative = narrative
        st.session_state.last_bedrock_update = now
        st.session_state.bedrock_history.append({
            'time': now.astimezone(cst).strftime('%I:%M %p CST'),
            'narrative': narrative
        })
        return narrative
        
    except Exception as e:
        return f"AI analysis unavailable: {e}"

def calculate_momentum_display(items, source):
    """
    Filters items by source and calculates
    if sentiment is trending up or down.
    Returns a color and label for the UI.
    """
    source_items = [i for i in items if i.get('source') == source]
    if len(source_items) < 2:
        return 0, "neutral"
    
    recent = float(source_items[-1].get('avg_positive', 0))
    previous = float(source_items[-2].get('avg_positive', 0))
    delta = round(recent - previous, 3)
    
    if delta > 0.02:
        return delta, "rising"
    elif delta < -0.02:
        return delta, "falling"
    else:
        return delta, "stable"

# ─── DASHBOARD LAYOUT ────────────────────────────────────────────

st.markdown("## NBA Finals Game 5 — Live Fan Sentiment")
st.markdown("Knicks vs Spurs · Real-time analysis via YouTube & Bluesky → Kinesis → Comprehend → Bedrock")

# Fetch all data
score = get_live_score()
sentiment_items = get_sentiment_data()
narrative = get_ai_narrative(sentiment_items, score)

# ─── SCORE ROW ───────────────────────────────────────────────────

st.markdown("---")
if score:
    away_score = int(score['away_score']) if score['away_score'] else 0
    home_score = int(score['home_score']) if score['home_score'] else 0
    leading = "winning" if away_score > home_score else "trailing"
    
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #1a1a2e 100%);
        border: 1px solid #333;
        border-radius: 16px;
        padding: 24px 32px;
        text-align: center;
        margin-bottom: 8px;
    ">
        <div style="font-size:12px; color:#888; letter-spacing:0.15em; text-transform:uppercase; margin-bottom:16px">
            🏀 NBA Finals 2026 · Game 5 · {score['status']}
        </div>
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <div style="flex:1; text-align:left;">
                <div style="font-size:13px; color:#aaa; margin-bottom:4px">Away</div>
                <div style="font-size:18px; font-weight:600; color:white; margin-bottom:8px">{score['away_team']}</div>
                <div style="font-size:56px; font-weight:700; color:{'#4ade80' if away_score > home_score else 'white'}; line-height:1">{score['away_score']}</div>
            </div>
            <div style="flex:0; padding:0 32px; text-align:center;">
                <div style="font-size:13px; color:#888; margin-bottom:4px">Q{score['quarter']}</div>
                <div style="font-size:24px; font-weight:500; color:#555">vs</div>
                <div style="font-size:20px; font-weight:600; color:#e879f9; margin-top:4px">{score['clock']}</div>
            </div>
            <div style="flex:1; text-align:right;">
                <div style="font-size:13px; color:#aaa; margin-bottom:4px">Home</div>
                <div style="font-size:18px; font-weight:600; color:white; margin-bottom:8px">{score['home_team']}</div>
                <div style="font-size:56px; font-weight:700; color:{'#4ade80' if home_score > away_score else 'white'}; line-height:1">{score['home_score']}</div>
            </div>
        </div>
        <div style="margin-top:16px; font-size:12px; color:#666;">
            {'🟢 ' + score['away_team'].split()[-1] + ' leading by ' + str(abs(away_score-home_score)) if away_score > home_score else '🟢 ' + score['home_team'].split()[-1] + ' leading by ' + str(abs(away_score-home_score)) if home_score > away_score else '🟡 Tied game'}
        </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.info("Game not live yet — score will appear at tip-off")

# ─── METRIC STRIP ────────────────────────────────────────────────

st.markdown("---")
m1, m2, m3, m4 = st.columns(4)

current_comments = sum(int(i.get('total_comments', 0)) for i in sentiment_items)
st.session_state.total_comments_all_time = max(
    st.session_state.total_comments_all_time, 
    st.session_state.total_comments_all_time + max(0, current_comments - st.session_state.get('last_comment_count', 0))
)
st.session_state.last_comment_count = current_comments
total_comments = st.session_state.total_comments_all_time
all_positive = [float(i.get('avg_positive', 0)) for i in sentiment_items]
overall_sentiment = round(sum(all_positive) / len(all_positive) * 100, 1) if all_positive else 0

youtube_items = [i for i in sentiment_items if 'youtube' in i.get('source', '')]
bluesky_items = [i for i in sentiment_items if 'bluesky' in i.get('source', '')]

m1.metric("Total comments", f"{total_comments:,}")
m2.metric("Overall positive", f"{overall_sentiment}%")
m3.metric("YouTube windows", len(youtube_items))
m4.metric("Bluesky windows", len(bluesky_items))

# ─── CHARTS ROW ──────────────────────────────────────────────────

st.markdown("---")
chart_col, bar_col = st.columns(2)

with chart_col:
    st.markdown("#### Sentiment over time")
    
    if sentiment_items:
        df = pd.DataFrame(sentiment_items)
        df['avg_positive'] = df['avg_positive'].astype(float)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC').dt.tz_convert('America/Chicago').dt.tz_localize(None)
        df = df.sort_values('timestamp')
        
        # Separate by source for two lines
        youtube_df = df[df['source'].str.contains('youtube', na=False)]
        bluesky_df = df[df['source'].str.contains('bluesky', na=False)]
        
        chart_data = pd.DataFrame()
        if not youtube_df.empty:
            chart_data['YouTube'] = youtube_df.set_index('timestamp')['avg_positive']
        if not bluesky_df.empty:
            chart_data['Bluesky'] = bluesky_df.set_index('timestamp')['avg_positive']
        
        if not chart_data.empty:
            st.line_chart(chart_data)
    else:
        st.info("Sentiment data will appear once the pipeline is running")

with bar_col:
    st.markdown("#### Sentiment by source")
    
    if youtube_items:
        yt_avg = round(sum(float(i.get('avg_positive', 0)) for i in youtube_items) / len(youtube_items) * 100, 1)
        st.markdown(f"**YouTube** — {yt_avg}% positive")
        st.progress(yt_avg / 100)
    else:
        st.markdown("**YouTube** — waiting for data")
        st.progress(0.0)
    
    st.markdown("")
    
    if bluesky_items:
        bsky_avg = round(sum(float(i.get('avg_positive', 0)) for i in bluesky_items) / len(bluesky_items) * 100, 1)
        st.markdown(f"**Bluesky** — {bsky_avg}% positive")
        st.progress(bsky_avg / 100)
    else:
        st.markdown("**Bluesky** — waiting for data")
        st.progress(0.0)

    # Momentum indicators
    st.markdown("---")
    st.markdown("#### ⚡ Momentum")

    yt_delta, yt_direction = calculate_momentum_display(sentiment_items, 'youtube')
    bsky_delta, bsky_direction = calculate_momentum_display(sentiment_items, 'bluesky')

    direction_emoji = {"rising": "🚀 Rising", "falling": "📉 Falling", "stable": "➡️ Stable", "neutral": "➡️ Neutral"}

    col_yt, col_bsky = st.columns(2)

    with col_yt:
        st.metric(
            label="YouTube momentum",
            value=direction_emoji[yt_direction],
            delta=f"{yt_delta:+.3f}",
            delta_color="normal"
        )

    with col_bsky:
        st.metric(
            label="Bluesky momentum", 
            value=direction_emoji[bsky_direction],
            delta=f"{bsky_delta:+.3f}",
            delta_color="normal"
        )

# ─── LIVE COMMENT FEED ───────────────────────────────────────────
st.markdown("---")
st.markdown("#### 💬 What fans are saying right now")

if sentiment_items:
    # Pull raw recent comments from DynamoDB
    try:
        feed_response = table.scan(
            FilterExpression='#ts >= :cutoff AND attribute_exists(sample_comments)',
            ExpressionAttributeNames={'#ts': 'timestamp'},
            ExpressionAttributeValues={
                ':cutoff': (datetime.utcnow() - timedelta(minutes=15)).isoformat()[:16]
            },
            Limit=20
        )
        feed_items = feed_response.get('Items', [])
        
        if feed_items:
            for item in feed_items[-5:]:
                comments = item.get('sample_comments', [])
                for comment in comments[:2]:
                    sentiment = comment.get('sentiment', 'NEUTRAL')
                    text = comment.get('text', '')
                    source = item.get('source', '')
                    
                    color = {
                        'POSITIVE': '🟢',
                        'NEGATIVE': '🔴', 
                        'NEUTRAL': '⚪',
                        'MIXED': '🟡'
                    }.get(sentiment, '⚪')
                    
                    st.markdown(f"{color} **{source}** — {text}")
        else:
            st.caption("Comments will appear here as they're processed")
    except:
        st.caption("Loading comments...")
else:
    st.caption("Waiting for data...")

# ─── AI NARRATIVE ────────────────────────────────────────────────

st.markdown("---")
st.markdown("#### AI narrative — Bedrock analysis")

last_update = st.session_state.last_bedrock_update
update_str = last_update.astimezone(cst).strftime('%I:%M:%S %p CST') if last_update else "Not yet generated"

st.info(narrative)
st.caption(f"Last updated: {update_str} · Refreshes every 5 minutes")

# Narrative history expander
if st.session_state.bedrock_history:
    with st.expander("View narrative history"):
        for entry in reversed(st.session_state.bedrock_history[:-1]):
            st.markdown(f"**{entry['time']}** — {entry['narrative']}")
            st.markdown("---")

# ─── AUTO REFRESH ────────────────────────────────────────────────

st.markdown("---")
st.caption(f"Last refreshed: {datetime.now(cst).strftime('%H:%M:%S CST')} · Auto-refreshes every 30 seconds")

# Why 30 seconds?
# Fast enough to feel live during the game.
# Slow enough to not hammer DynamoDB with reads.
# Companies tune this based on data freshness needs.
time.sleep(30)
st.rerun()