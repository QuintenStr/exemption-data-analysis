
import praw
import re
import json
import pydoop.hdfs as hdfs
from datetime import datetime

print("Deleting existing comments in folder")
files = hdfs.ls("/Oefeningen/Project/Comments/")
for file in files:
    hdfs.rmr(file)

# Preprocessing van de text (zelfde code als in begin van het project)
def preprocess_tweet_text(tweet):
    tweet = re.sub(r"http\S+", "", tweet)
    tweet = tweet.replace("&amp;", "and")
    tweet = re.sub(r"@[^\s]+", "", tweet)
    tweet = re.sub(r"#(\w+)", "", tweet)
    tweet = re.sub(r"(^|\s)RT(\s|$)", " ", tweet)
    tweet = tweet.encode("ascii", "ignore").decode()
    emoji_pattern = re.compile("[" u"\U0001F600-\U0001F64F" u"\U0001F300-\U0001F5FF" u"\U0001F680-\U0001F6FF" u"\U0001F1E0-\U0001F1FF" u"\U00002702-\U000027B0" u"\U000024C2-\U0001F251" "]+", flags=re.UNICODE)
    tweet = emoji_pattern.sub(r"", tweet)
    tweet = re.sub(r"[^a-zA-Z0-9]", " ", tweet)
    tweet = tweet.lower()
    tweet = re.sub(r"\s+", " ", tweet).strip()
    return tweet

reddit = praw.Reddit(
    client_id="co084F0-3Ny8mDHZe0nSfw",
    client_secret="0_ri6wpokDxDoUXL_5IHv2gCaEDfug",
    user_agent="linuxvm:projectbigdataquintenlorenzo:v1.0.0",
    password="nCqarxBZ8n2eKLr",
    username="Illustrious-Ant9761",
)

reddit.read_only = True

# enkele subreddits genomen waar volgens:
# https://maxcandocia.com/article/2017/Jan/31/which-subreddits-swear-the-most/
# vaak in de comments gescholden wordt, als we andere gaan nemen kan het zijn dat we niet meteen
# veel offensive language comments vinden want er staan vast filters op wat je kan commenten op reddit

subreddit = reddit.subreddit("videos+The_Donald+tifu+news+Documentaries+AskReddit+worldnews+pics")

print("Writing comments to HDFS")
for comment in subreddit.stream.comments():
    cleaned_comment = preprocess_tweet_text(comment.body)
    if len(cleaned_comment.split()) > 10:
        try:
            comment = reddit.comment(comment.id)
            comment.refresh()
            comment.replies.replace_more()
            replies = comment.replies.__len__()
            comment_obj = {
                "id": comment.id,
                "author": comment.author.name,
                "tweet": cleaned_comment,
                "replies": replies,
                "score": comment.score,
                "submission_comment_count": comment.submission.num_comments,
                "submission_score": comment.submission.score,
                "timestamp": comment.created_utc
            }
            json_comment = json.dumps(comment_obj, ensure_ascii=False)
            with hdfs.open(f"/Oefeningen/Project/Comments/{comment.id}.json", "w") as f:
                f.write(json_comment.encode("utf-8"))
                f.close()
        except Exception as e:
            print(f"[{datetime.now()}] An error occurred: {str(e)}, comment skipped")                
