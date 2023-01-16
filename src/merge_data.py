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
class Sum:
    bikes: int
    fuel_price: FuelPrice
    weather_data: Weather

fuel_data = []

merged_data_topic = "merged_data"
accumulated_data_topic = "accumulated_data"


if __name__ == "__main__":
    bike_consumer = KafkaConsumer(config.BIKE_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    bike_done = KafkaConsumer("bike_done", bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    weather_consumer = KafkaConsumer(config.WEATHER_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    
    fuel_consumer = KafkaConsumer(config.FUEL_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    counted_bikes = 0
    history = []
    timestamp = 0
    timestamps = []

    producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))

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
                merge = MergedData(bike_data, fuel_price_data, weather_data)
                timestamp = bike_data.time

                # json_data = json.dumps(merge)
                # future = producer.send(merged_data_topic, json_data)
                # future.get(timeout=10)

                # publish merged data to kafkatea
                summed_data = Sum(counted_bikes, fuel_price_data, weather_data)

                # future = producer.send(accumulated_data_topic, summed_data.__dict__)
                # future.get(timeout=10)
                
        messages = bike_done.poll(timeout_ms=10)
        for message in messages:  # update fuel data object
            history.append(counted_bikes)
            timestamps.append(timestamp)
            counted_bikes = 0
            timestamp = 0
            # plt.plot(history)
            plt.plot(timestamps, history)
            plt.xticks(np.arange(len(timestamps)), timestamps, rotation=45, ha='right')
            plt.xlabel('Counted Bikes accumulated')
            plt.ylabel('Time 5 Min Interval')
            plt.title('Gezählte Fahrräder')
            # plt.ion()
            plt.show()