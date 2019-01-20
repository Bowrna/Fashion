import json
import http.client

conn = http.client.HTTPConnection("localhost:8983")

url = "/solr/fashion/update/json/docs"
retrieve_url = lambda count : "/solr/fashion/select?q=*:*&rows="+str(count)+"&sort=timestamp%20desc"
headers = {
    'content-type': "application/json",
    'cache-control': "no-cache"
    }


def storeTweet(tweet):
    tweet_data = {}
    tweet_data['tweet'] = tweet
    payload = json.dumps(tweet_data)
    conn.request("POST",url , payload, headers)

    res = conn.getresponse()
    data = res.read()

    print(data.decode("utf-8"))

def retrieveTweet():
    conn.request("GET", retrieve_url(100), headers=headers)
    res = conn.getresponse()
    data = res.read()
    #print(data.decode("utf-8"))
    return data
