
import praw

# Replace with your actual credentials
reddit = praw.Reddit(
    client_id='P2-GxeB4rM5zNXzoUGCBvg',
    client_secret='F7rxCrpQ97ZxnarmLESaK2hNmalZlw',
    user_agent='the ACE'
)

# Search queries
queries = [
    "construction",
    "civil engineering",
    "architecture and design",
    "building",
    "bridges construction",
    "tenders",
    "AEC industry"
]

# Store results
results = []

for query in queries:
    print(f"\n🔍 Query: {query}")
    for submission in reddit.subreddit("all").search(query, sort="new", limit=20):
        result = {
            "title": submission.title,
            "url": submission.url,
            "score": submission.score,
            "subreddit": submission.subreddit.display_name,
            "created_utc": submission.created_utc,
            "selftext": submission.selftext,
        }
        results.append(result)
        print(f"  - {submission.title}")

# Optional: Save to CSV or JSON
import pandas as pd
df = pd.DataFrame(results)
df.to_csv("reddit_aec_data.csv", index=False)
