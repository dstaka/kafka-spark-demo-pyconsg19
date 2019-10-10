import json, config
from requests_oauthlib import OAuth1Session
import pandas as pd
import time
import configparser
from kafka import KafkaProducer


# Read Twitter API credentials information from config file
config = configparser.RawConfigParser()
config.read('./config/twitter_credentials.conf')
CONSUMER_KEY = config.get('Twitter_API', 'CONSUMER_KEY')
CONSUMER_SECRET = config.get('Twitter_API', 'CONSUMER_SECRET')
ACCESS_TOKEN = config.get('Twitter_API', 'ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = config.get('Twitter_API', 'ACCESS_TOKEN_SECRET')
endpoint_url = "https://api.twitter.com/1.1/statuses/home_timeline.json"

# Set Kafka config
kafka_broker_hostname='localhost'
kafka_broker_portno='9092'
kafka_broker=kafka_broker_hostname + ':' + kafka_broker_portno
kafka_topic='topic-tweet-raw'
producer = KafkaProducer(bootstrap_servers=kafka_broker)

# Set sleep interval
sleep_interval=30

# Set how many tweets to extract
params ={'count' : 100}


if __name__ == "__main__":
    # Authenticate Twitter API
    twitter = OAuth1Session(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    while True :
        res = twitter.get(endpoint_url, params = params)

        # Parse JSON data from Twitter API and send to Kafka
        if res.status_code == 200:
            # Extract tweets of a timeline
            timelines = json.loads(res.text)
            # Parse JSON in each tweet to send to Kafka
            for line_dict in timelines:
                # Remove retweet records
                if "RT @" in line_dict['text']:
                    print('skip!!!')
                    continue
                print('***')
                kafka_msg_str=''
                line_str = json.dumps(line_dict)
                kafka_msg_str = (kafka_msg_str + '{ "tweet_id" : ' + line_dict['id_str'] + ', "tweet_property" : { '
                    + '"user_name" : "' + line_dict['user']['name'] + '", '
                    + '"tweet" : "' + line_dict['text'].replace('\n', ' ').replace('"', ' ') + '", '
                    + '"created_at" : "' + line_dict['created_at']
                    + '"} }\n')
                print(kafka_msg_str)
                producer.send(kafka_topic, bytes(kafka_msg_str, 'utf-8'))
        else:
            print("Failed: %d" % res.status_code)
        
        time.sleep(sleep_interval)