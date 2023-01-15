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
    for _ in range(2):
        producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        future: FutureRecordMetadata = producer.send(KAFKA_TOPIC, {"value1": 5.8})
        future.get(timeout=10)
