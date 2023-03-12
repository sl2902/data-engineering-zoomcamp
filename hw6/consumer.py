import pandas as pd
import os
from typing import Dict, List
from json import loads
from kafka import KafkaConsumer

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC


class JsonConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: Dict):
        self.consumer.subscribe(list(topics.keys()))
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                for message_key, message_value in message.items():
                    for rides in message_value:
                        print(rides.topic, rides.key)
                        # if rides.topic == "fhv_rides":
                        #     print(pd.DataFrame(rides.value).columns)
                        ride = pd.DataFrame(rides.value)
                        print(ride.columns)
                        print(ride['Zone'].value_counts())
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        'enable_auto_commit': False,
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        'value_deserializer': lambda x: loads(x.decode('utf-8')),
        'group_id': 'consumer.group.id.taxi.1',
    }

    json_consumer = JsonConsumer(props=config)
    json_consumer.consume_from_kafka(topics=KAFKA_TOPIC)