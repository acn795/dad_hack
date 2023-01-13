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

class FuelPrice:
    STATION_ID = "2f0d5ed0-7935-4d5e-b623-aa20ce2ba334"
    API_KEY = "36d397b8-02e3-a4f8-597e-ff8d58fe59a4"
    KAFKA_SERVER = "localhost:9092"
    KAFKA_TOPIC = "fuel_price_2"

    @dataclass
    class Price:
        date: float
        e5: float
        e10: float
        diesel: float

    def load_from_external_api(self):
        resp_json = requests.get(f"https://creativecommons.tankerkoenig.de/json/prices.php?ids={self.STATION_ID}&apikey={self.API_KEY}")

        if resp_json.ok:
            data = json.loads(resp_json.content)
            prices_dict = data["prices"][self.STATION_ID]
            prices = self.Price(time.time(), prices_dict["e5"], prices_dict["e10"], prices_dict["diesel"])

            print(prices.__dict__)

            return prices
        
        return self.load_from_external_api()
    
    def write_to_kafka(self, prices: Price):
        producer = KafkaProducer(bootstrap_servers=self.KAFKA_SERVER)
        future: FutureRecordMetadata = producer.send(self.KAFKA_TOPIC, (json.dumps(prices.__dict__).encode('utf-8')))
        future.get(timeout=10)

    def subscribe_to_kafka(self):
        consumer = KafkaConsumer(self.KAFKA_TOPIC, bootstrap_servers=self.KAFKA_SERVER)
        
        for message in consumer:
            prices_dict = message.value.decode("utf-8")
            print(json.loads(message.value.decode("utf-8")))
            # prices = self.Prices(time.time(), prices_dict["e5"], prices_dict["e10"], prices_dict["diesel"])

if __name__ == "__main__":
    fuel_prices = FuelPrice()

    fuel_prices.subscribe_to_kafka()

    while True:
        prices = fuel_prices.load_from_external_api()
        if prices:
            fuel_prices.write_to_kafka(prices)
        time.sleep(5)
