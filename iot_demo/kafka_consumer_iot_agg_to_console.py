# Usage: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 kafka_consumer_iot_agg_to_console.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaConsumer

# Set Kafka config
kafka_broker_hostname='localhost'
kafka_consumer_portno='9092'
kafka_broker=kafka_broker_hostname + ':' + kafka_consumer_portno
kafka_topic_input='topic-iot-raw'


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

    # Print schema information as a header
    df_kafka_string_parsed_formatted_timestamped.printSchema()

    # Compute average of speed, accelerometer_x, accelerometer_y, and accelerometer_z during 5 minutes
    # Data comes after 10 minutes will be ignores
    df_windowavg = df_kafka_string_parsed_formatted_timestamped.withWatermark("timestamp", "10 minutes").groupBy(
        window(df_kafka_string_parsed_formatted_timestamped.timestamp, "5 minutes"),
        df_kafka_string_parsed_formatted_timestamped.device_id).avg("speed", "accelerometer_x", "accelerometer_y", "accelerometer_z")

    # Add columns showing each window start and end timestamp
    df_windowavg_timewindow = df_windowavg.select(
        "device_id",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg(speed)").alias("avg_speed"),
        col("avg(accelerometer_x)").alias("avg_accelerometer_x"),
        col("avg(accelerometer_y)").alias("avg_accelerometer_y"),
        col("avg(accelerometer_z)").alias("avg_accelerometer_z")
        ).orderBy(asc("device_id"), asc("window_start"))

    # Print output to console
    query_console = df_windowavg_timewindow.writeStream.outputMode("complete").format("console").start()
    query_console.awaitTermination()
