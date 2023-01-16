#!/usr/bin/env python3.10

import json
import time
import numpy as np
import requests

from dataclasses import dataclass
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.producer.future import FutureRecordMetadata
from datetime import timedelta
import matplotlib.pyplot as plt

import config
from producers.bike import BikeSensor5Min
from producers.fuel_price import FuelPrice
from producers.weather import Weather

@dataclass
class MergedData:
    bike_data: BikeSensor5Min
    fuel_price_data: FuelPrice
    weather_data: Weather

@dataclass
class AccumulatedData:
    bikes: int
    fuel_price: FuelPrice
    weather_data: Weather


if __name__ == "__main__":
    # Producer
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    # Consumer
    bike_consumer = KafkaConsumer(config.BIKE_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    bike_done_consumer = KafkaConsumer(config.BIKE_DONE_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    weather_consumer = KafkaConsumer(config.WEATHER_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    fuel_consumer = KafkaConsumer(config.FUEL_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Lists
    counted_bikes = 0
    history = []
    timestamp = 0
    timestamps = []

    fuel_price_data: FuelPrice
    bike_data: BikeSensor5Min
    weather_data: Weather

    while True:
        # publish merged data to kafka for grafana

        messages = weather_consumer.poll(timeout_ms=10)
        for msg in messages:  # update fuel data object
            for value in messages[msg]:
                # print(value.value)
                weather_data = Weather(
                    value.value["temp"],
                    value.value["dwpt"],
                    value.value["rhum"],
                    value.value["prcp"],
                    value.value["snow"],
                    value.value["wdir"],
                    value.value["wspd"],
                    value.value["wpgt"],
                    value.value["pres"],
                    value.value["tsun"],
                    value.value["coco"],
                )

        messages = fuel_consumer.poll(timeout_ms=10)
        for msg in messages:  # update fuel data object
            for value in messages[msg]:
                fuel_price_data = FuelPrice(
                    value.value["temp"],
                    value.value["e5"],
                    value.value["e10"],
                    value.value["diesel"],
                )

        messages = bike_consumer.poll(timeout_ms=200)
        for msg in messages: # merge data with bike sensors
            for value in messages[msg]:
                bike_data = BikeSensor5Min(
                    value.value["id"],
                    value.value["name"],
                    value.value["count"],
                    value.value["time"],
                    value.value["coordinateType"],
                    value.value["coordinates"],
                )
                counted_bikes += bike_data.count

                # Publish merged data
                merge = MergedData(bike_data, fuel_price_data, weather_data)
                future = producer.send(config.MERGED_DATA_TOPIC, merge)
                future.get(timeout=10)

        # Publish accumulated data to kafka
        accumulated_data = AccumulatedData(counted_bikes, fuel_price_data, weather_data)
        future = producer.send(config.ACCUMULATED_DATA_TOPIC, accumulated_data.__dict__)
        future.get(timeout=10)