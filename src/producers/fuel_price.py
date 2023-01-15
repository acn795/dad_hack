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

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

@dataclass
class FuelPrice:
    date: float
    e5: float
    e10: float
    diesel: float

class FuelPriceProducer:
    def load_from_external_api(self):
        resp_json = requests.get(f"https://creativecommons.tankerkoenig.de/json/prices.php?ids={config.FUEL_STATION_ID}&apikey={config.FUEL_API_KEY}")

        if resp_json.ok:
            data = json.loads(resp_json.content)
            prices_dict = data["prices"][config.FUEL_STATION_ID]
            prices = FuelPrice(time.time(), prices_dict["e5"], prices_dict["e10"], prices_dict["diesel"])

            print(prices.__dict__)

            return prices
        
        return self.load_from_external_api()
    
    def write_to_kafka(self, prices: FuelPrice):
        producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVER)
        future: FutureRecordMetadata = producer.send(config.FUEL_TOPIC, (json.dumps(prices.__dict__).encode('utf-8')))
        future.get(timeout=10)

if __name__ == "__main__":
    producer = FuelPriceProducer()

    while True:
        prices = producer.load_from_external_api()
        if prices:
            producer.write_to_kafka(prices)
        time.sleep(60*60)
