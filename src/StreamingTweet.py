#!/usr/bin/python
from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json
# Go to http://apps.twitter.com and create an app.
# The consumer key and secret will be generated for you after
consumer_key="USE_YOUR_CONSUMER_KEY"
consumer_secret="USE_YOUR_CONSUMER_SECRET_KEY"

# After the step above, you will be redirected to your app's page.
# Create an access token under the the "Your access token" section
access_token="USE_YOUR_ACCESS_TOKEN"
access_token_secret="USE_YOUR_ACCESS_TOKEN_SECRET"
producer = KafkaProducer(bootstrap_servers='localhost:9092')

class StdOutListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)

class FileListener(StreamListener):
    """ A listener handles tweets that are received from the stream.
    This is a file writing listener that just writes received tweets to file.
    Have included it for testing purpose
    """
    def on_data(self, data):
        print(data)
        f = open("output.txt","a+")
        f.write(data)
        f.close()
        return True

    def on_error(self, status):
        print(status)

class KafkaDataPushListener(StreamListener):
    def on_data(self, data):
        print(data)
        producer.send('fashion',json.dumps(data).encode('utf-8'))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    l = KafkaDataPushListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    stream = Stream(auth, l)
    stream.filter(track=['#fashion'])
