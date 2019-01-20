from flask import Flask,jsonify
from src.StorageUtil import  retrieveTweet
from src.TweetStats import  getTopHashTags,getHashTagCountLastHour
import json
app = Flask(__name__)

@app.route("/top_hashtags")
def hashtags():
    result = getTopHashTags(10)
    return jsonify(result)
@app.route("/tweets_count_per_minute")
def tweetcount():
    result = getHashTagCountLastHour()
    return jsonify(result)

@app.route("/show_tweets/")
def showtweets():
    result = retrieveTweet()
    output = json.loads(result)
    response = (output["response"])
    out = response["docs"]
    return jsonify(out)

if __name__ == '__main__':
    app.run(debug=True)


