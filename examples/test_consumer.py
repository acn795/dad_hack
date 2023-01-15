#!/usr/bin/env python3.10

import json
import time
import numpy 
import requests

from dataclasses import dataclass
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.producer.future import FutureRecordMetadata
from datetime import timedelta

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "mannnnnnno"

if __name__ == "__main__":
    consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for message in consumer:
        print(message.value)
