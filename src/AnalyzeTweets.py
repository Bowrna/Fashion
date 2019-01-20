from kafka import KafkaConsumer
from src.TweetStats import *
from src.StorageUtil import *

def parseTweet(tweet,key,tweeted_time):
    #print("In parse tweet:"+json.dumps(tweet))
    tweet_msg = tweet[key]
    print("Entire tweet:"+tweet_msg)
    hashtags = tweet['entities']['hashtags']
    tags = {tag["text"]:1 for tag in hashtags}
    print(tags)
    setHashTags(tags,float(tweeted_time))
    storeTweet(tweet_msg)

consumer = KafkaConsumer('fashion',bootstrap_servers='localhost:9092')
for msg in consumer:
    tweet_string=msg.value
    tweet = json.loads(tweet_string.decode('utf-8'))
    tweets = json.loads(tweet)
    time_stamp = tweets["timestamp_ms"]
    if 'extended_tweet' in tweets:
        extended_tweet = tweets["extended_tweet"]
        parseTweet(extended_tweet,"full_text",time_stamp)
    elif 'retweeted_status' in tweets:
        retweeted = tweets["retweeted_status"]
        if 'extended_tweet' in retweeted:
            parseTweet(retweeted["extended_tweet"],"full_text",time_stamp)
        else:
            parseTweet(retweeted,"text",time_stamp)
    else:
        parseTweet(tweets,"text",time_stamp)
