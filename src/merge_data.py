#!/usr/bin/env python3.10

import json
import time
import numpy as np
import requests

from dataclasses import dataclass
from dataclasses_json import dataclass_json
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.producer.future import FutureRecordMetadata
from datetime import timedelta
import matplotlib.pyplot as plt

import config
from producers.bike import BikeSensor5Min
from producers.fuel_price import FuelPrice
from producers.weather import Weather

@dataclass_json
@dataclass
class MergedData:
    bike_data: BikeSensor5Min
    fuel_price_data: FuelPrice
    weather_data: Weather

@dataclass_json
@dataclass
class AccumulatedData:
    timestamp: str
    bikes: int
    fuel_price: FuelPrice
    weather_data: Weather


if __name__ == "__main__":
    # Producer
    producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVER, value_serializer=lambda m: m.encode('utf-8'))

    # Consumer
    consumer = KafkaConsumer(
        bootstrap_servers=config.KAFKA_SERVER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    consumer.subscribe([
        config.BIKE_TOPIC,
        config.BIKE_DONE_TOPIC,
        config.WEATHER_TOPIC,
        config.FUEL_TOPIC
    ])

    # Lists
    counted_bikes = 0
    bike_timestamp = ""

    fuel_price_data = None
    bike_data = None
    weather_data = None

    while True:
        # publish merged data to kafka for grafana

        raw_messages = consumer.poll(timeout_ms=1_000 * 60 * 6) # 6 minutes
        for topic_partition, messages in raw_messages.items():
            topic = topic_partition.topic
            if topic == config.WEATHER_TOPIC:
                for value in messages:
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
            elif topic == config.FUEL_TOPIC:
                for value in messages:
                    fuel_price_data = FuelPrice(
                        value.value["date"],
                        value.value["e5"],
                        value.value["e10"],
                        value.value["diesel"],
                    )
            elif topic == config.BIKE_TOPIC:
                for value in messages:
                    bike_data = BikeSensor5Min(
                        value.value["id"],
                        value.value["name"],
                        value.value["count"],
                        value.value["time"],
                        value.value["coordinateType"],
                        value.value["coordinates"],
                    )

                    counted_bikes += bike_data.count
                    bike_timestamp = bike_data.time

                    # Publish merged data
                    if counted_bikes != None and fuel_price_data != None and weather_data != None:
                        merge = MergedData(bike_data, fuel_price_data, weather_data)
                        future = producer.send(config.MERGED_DATA_TOPIC, merge.to_json())
                        future.get(timeout=10)

            elif topic == config.BIKE_DONE_TOPIC:
                # Publish accumulated data to kafka
                if counted_bikes != None and fuel_price_data != None and weather_data != None:
                    accumulated_data = AccumulatedData(bike_timestamp, counted_bikes, fuel_price_data, weather_data)
                    print(accumulated_data.to_json())
                    future = producer.send(config.ACCUMULATED_DATA_TOPIC, accumulated_data.to_json())
                    future.get(timeout=10)
                
                counted_bikes = 0