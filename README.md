# Fashion
Simple analysing tool for analysing fashion hashtag from Twitter.

To Run this project:

1) Download the source and include all the third party libraries listed out in requirements.txt
2) Create an app in the Twitter and get access token and its secret, consumer token and its secret too.
3) Install the latest version of Kafka(it is running in my port 9092), Solr(running in my port 8983) and Redis(running in my port 6379) in your server
4) For this project, i will have installed them all in my local machine. 
5) After that replace the token in StreamingTwitter file and start the file. It will act as Producer to Kafka
6)Start running the file AnalyzeTweets in the next step. It will act as consumer and feeds the data to Solr and Redis
7)Run the flask app by running PullData file. Now you can pull the tweet details collected
