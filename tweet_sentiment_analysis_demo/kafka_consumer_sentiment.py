# Usage: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 kafka_consumer_sentiment.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer
import pandas as pd
import MeCab
import re
import codecs


# Set Kafka config
kafka_broker_hostname='localhost'
kafka_broker_portno='9092'
kafka_broker=kafka_broker_hostname + ':' + kafka_broker_portno
kafka_topic_input='topic-tweet-raw'


class JapaneseTokenizer(object):
    def __init__(self):
        # Use Mecab with mecab-ipadic-NEologd
        self.mecab = MeCab.Tagger('-Ochasen -d /usr/local/lib/mecab/dic/mecab-ipadic-neologd')
        self.mecab.parseToNode('')
        self.mecab.parse('')
        
    def split_text(self, text: str):
        node = self.mecab.parseToNode(text)
        words = []
        while node:
            if node.surface:
                words.append(node.surface)
            node = node.next
        return words
    
    # Create list of words
    def create_wordlist(self, text: str):
        # Parse whole tweets while removing URL characters from a tweet
        parsed_text = self.mecab.parse(re.sub('https?://[\w/:%#\$&\?\(\)~\.=\+\-]+', ' ', text))
        # Split tweet by line
        word_list = parsed_text.split('\n')
        # Extract noun from each tweet
        word_list = [line.split('\t')[0] for line in word_list]
        word_list = [word for word in word_list[:-2] if not word.isnumeric()]
        return word_list


# Define tokenize function as UDF to run on Spark (pyspark.sql.functions.udf)
def tokenize(text: str):
    tokenizer = JapaneseTokenizer()
    return tokenizer.create_wordlist(text)
udf_tokenize = udf(tokenize)


# Create a polarity dictionary from pn_ja.dic file downloaded from http://www.lr.pi.titech.ac.jp/~takamura/pubs/pn_ja.dic
# The polarity dictionary consists of key (each Japanese word) and value (polarity score)
def load_pn_dict():
    dic = {}
    with codecs.open('./pn_ja.dic', 'r', 'utf-8') as f:
        lines = f.readlines()
        for line in lines:
            columns = line.split(':')
            dic[columns[0]] = float(columns[3])
    return dic
pn_dic = load_pn_dict()


class CorpusElement:
    def __init__(self, text='', tokens=[], pn_scores=[]):
        # Tweet body
        self.text = text
        # List of tokens
        self.tokens = tokens
        # Polarity score
        self.pn_scores = pn_scores


# Define score function as UDF to run on Spark (pyspark.sql.functions.udf)
# Derive polarity score of words from a list of tokens
def get_pn_scores(tokens):
    counter=1
    scores=0
    for surface in tokens:
        if surface in pn_dic:
            counter=counter+1
            scores=scores+(pn_dic[surface])
    return scores/counter
udf_get_pn_scores = udf(get_pn_scores)


if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder.appName("tweet_sentiment_analysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    ## Input from Kafka
    # Pull data from Kafka topic
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic_input) \
        .load()

    # Convert data from Kafka into String type
    df_kafka_string = df_kafka.selectExpr("CAST(value AS STRING) as value")

    # Define schema to read JSON format data
    # Deal with nested structure
    tweet_property_schema = StructType() \
        .add("user_name", StringType()) \
        .add("tweet", StringType()) \
        .add("created_at", StringType())
    tweet_schema = StructType().add("tweet_id", LongType()).add("tweet_property", tweet_property_schema)

    # Parse JSON data
    df_kafka_string_parsed = df_kafka_string.select(
        from_json(df_kafka_string.value, tweet_schema).alias("tweet_data"))
    df_kafka_string_parsed_formatted = df_kafka_string_parsed.select(
        col("tweet_data.tweet_id").alias("tweet_id"),
        col("tweet_data.tweet_property.user_name").alias("user_name"),
        col("tweet_data.tweet_property.tweet").alias("tweet"),
        col("tweet_data.tweet_property.created_at").alias("created_at"))

    # Tokenize
    df_kafka_string_parsed_formatted_tokenized = df_kafka_string_parsed_formatted.select('*', udf_tokenize('tweet').alias('tweet_tokenized'))

    # Compute sentiment score
    df_kafka_string_parsed_formatted_score = df_kafka_string_parsed_formatted_tokenized.select(
        'tweet_id',
        'user_name',
        'tweet',
        'created_at',
        udf_get_pn_scores('tweet_tokenized').alias('sentiment_score'))

    # Print output to console
    query_console = df_kafka_string_parsed_formatted_score.writeStream.outputMode("append").format("console").start()
    # Process data until termination signal is received
    query_console.awaitTermination()
