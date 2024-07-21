from kafka import KafkaProducer
import json
import time
from config import KAFKA_BROKER, KAFKA_TOPIC
from collect_data import DataCollectorBuilder
import random

data_collector = DataCollectorBuilder().build()

def kafka_producer():
    """
    Send stock market data to a Kafka topic.
    """
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    symbol = 'AAPL'
    
    while True:
        stocks = data_collector.get_data()
        if stocks is not None:
            for i in range(len(stocks)):
                message = stocks.iloc[i].to_dict()
                message['id'] = random.randint(1, 99999999)
                producer.send(KAFKA_TOPIC, message)
                print(f"Sent data for {symbol} to Kafka")
                time.sleep(1)
        else:
            print(f"Failed to fetch data for {symbol}")
        
if __name__ == "__main__":
    kafka_producer()
