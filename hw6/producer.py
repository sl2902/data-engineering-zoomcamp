

import pandas as pd
import os
import json
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC, LOOKUP_FILE


class JsonProducer(KafkaProducer):
    counter = 0
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)
    
    @ staticmethod
    def read_zone_lookup(filepath: str) -> pd.DataFrame:
        return pd.read_csv(filepath)

 #   @staticmethod
    def read_records(self, resource_path: Dict, lookup_df: pd.DataFrame, chunksize=1000):
        for topic_name, filepath in resource_path.items():
            chunks = pd.read_csv(filepath, compression="gzip", chunksize=chunksize)
            for chunk in chunks:
                if filepath.split('/')[-1].startswith("green"):
                    chunk["pickup_datetime"] = pd.to_datetime(chunk["lpep_pickup_datetime"])
                    chunk["dropoff_datetime"] = pd.to_datetime(chunk["lpep_dropoff_datetime"])
                elif filepath.split('/')[-1].startswith("fhv"):
                    chunk["pickup_datetime"] = pd.to_datetime(chunk["pickup_datetime"])
                    chunk["dropoff_datetime"] = pd.to_datetime(chunk["dropOff_datetime"])
                    chunk = chunk.rename(columns={'PUlocationID': 'PULocationID'})
                else:
                    raise ValueError("Invalid filename")
                # key = str(JsonProducer.counter).encode('utf8')
                chunk = chunk.merge(lookup_df, left_on=["PULocationID"], right_on=["LocationID"], how="left")
                # value = json.dumps(chunk.to_dict(), default=str).encode('utf8')
                self.publish_rides(topic=topic_name, key=JsonProducer.counter, messages=chunk)
                JsonProducer.counter += 1

    def publish_rides(self, topic: str, key: int, messages: pd.DataFrame):
        try:
            record = self.producer.send(topic=topic, key=key, value=messages)
            print('Record {} successfully produced at offset {}'.format(key, record.get().offset))
        except KafkaTimeoutError as e:
            print(e.__str__())


if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'max_request_size': 3173440261,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    producer = JsonProducer(props=config)
    lookup = producer.read_zone_lookup(LOOKUP_FILE)
    producer.read_records(resource_path=KAFKA_TOPIC, lookup_df=lookup)
    # producer.publish_rides(topic=[KAFKA_TOPIC], messages=rides)