import pandas as pd
import time
from kafka import KafkaProducer

# Set Kafka config
kafka_broker_hostname='localhost'
kafka_broker_portno='9092'
kafka_broker=kafka_broker_hostname + ':' + kafka_broker_portno
kafka_topic='topic-iot-raw'

data_send_interval=5


if __name__ == "__main__":
    # Create KafkaProducer instance
    producer = KafkaProducer(bootstrap_servers=kafka_broker)

    # Load demo data
    iot_data_id10 = pd.read_csv('./data/iot_data_id10.csv')
    iot_data_id11 = pd.read_csv('./data/iot_data_id11.csv')
    iot_data_id12 = pd.read_csv('./data/iot_data_id12.csv')

    # Send demo data to Kafka broker
    for _index in range(0, len(iot_data_id10)):
        json_iot_id10 = iot_data_id10[iot_data_id10.index==_index].to_json(orient='records')
        producer.send(kafka_topic, bytes(json_iot_id10, 'utf-8'))
        print(json_iot_id10)
        json_iot_id11 = iot_data_id11[iot_data_id11.index==_index].to_json(orient='records')
        producer.send(kafka_topic, bytes(json_iot_id11, 'utf-8'))
        print(json_iot_id11)
        json_iot_id12 = iot_data_id12[iot_data_id12.index==_index].to_json(orient='records')
        producer.send(kafka_topic, bytes(json_iot_id12, 'utf-8'))
        print(json_iot_id12)
        time.sleep(data_send_interval)
