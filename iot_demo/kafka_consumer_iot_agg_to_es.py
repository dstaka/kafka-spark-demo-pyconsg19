# Usage: spark-submit --jars ./elasticsearch-hadoop-7.1.1/dist/elasticsearch-hadoop-7.1.1.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 kafka_consumer_iot_agg_to_es.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer

# Set Kafka config
kafka_broker_hostname='localhost'
kafka_consumer_portno='9092'
kafka_broker=kafka_broker_hostname + ':' + kafka_consumer_portno
kafka_topic_input='topic-iot-raw'

# Set Elasticsearch config
es_hostname='localhost'
es_portno='9200'
es_doc_type_name='doc-iot-demo/doc'


if __name__ == "__main__":
    # Create Spark session
    spark = SparkSession.builder.appName("AggregateIoTdata").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Pull data from Kafka topic
    consumer = KafkaConsumer(kafka_topic_input)
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", kafka_topic_input) \
        .load()

    # Convert data from Kafka broker into String type
    df_kafka_string = df_kafka.selectExpr("CAST(value AS STRING) as value")

    # Define schema to read JSON format data
    iot_schema = StructType() \
        .add("device_id", LongType()) \
        .add("timestamp", StringType()) \
        .add("speed", DoubleType()) \
        .add("accelerometer_x", DoubleType()) \
        .add("accelerometer_y", DoubleType()) \
        .add("accelerometer_z", DoubleType())

    # Parse JSON data
    df_kafka_string_parsed = df_kafka_string.select(from_json(df_kafka_string.value, iot_schema).alias("iot_data"))

    df_kafka_string_parsed_formatted = df_kafka_string_parsed.select(
        col("iot_data.device_id").alias("device_id"),
        col("iot_data.timestamp").alias("timestamp"),
        col("iot_data.speed").alias("speed"),
        col("iot_data.accelerometer_x").alias("accelerometer_x"),
        col("iot_data.accelerometer_y").alias("accelerometer_y"),
        col("iot_data.accelerometer_z").alias("accelerometer_z"))

    # Convert timestamp field from string to Timestamp format
    df_kafka_string_parsed_formatted_timestamped = df_kafka_string_parsed_formatted.withColumn("timestamp", to_timestamp(df_kafka_string_parsed_formatted.timestamp, 'yyyy-MM-dd HH:mm:ss'))

    query_es = df_kafka_string_parsed_formatted_timestamped \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("es") \
        .outputMode("append") \
        .option("es.nodes", es_hostname) \
        .option("es.port", es_portno) \
        .option("checkpointLocation", "checkpoint/send_to_es") \
        .option('es.resource', es_doc_type_name) \
        .start("orders/log")
    query_es.awaitTermination()

# Check Elastichsearch from command line
# curl http://localhost:9200/doc-iot-demo/_search?pretty
# curl http://localhost:9200/doc-iot-demo/_search?pretty
