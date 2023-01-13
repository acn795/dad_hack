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

@dataclass
class Price:
    date: float
    e5: float
    e10: float
    diesel: float

@dataclass
class BikeSensor5Min:
    id: int
    name: str
    count: int
    time: str
    coordinateType: str
    coordinates: list

@dataclass
class MergedData:
    bike_data: BikeSensor5Min
    price_data: Price
    # fuel_data: 

@dataclass
class sum:
    price_data: Price
    # fuel_data: 
    bikes: int

weather_data = []
weather_topic = ""
bike_data = []
bike_topic = "bike_data_5_min"
fuel_data = []
fuel_topic = "fuel_price_2"

merged_data_topic = "merged_data"
accumulated_data_topic = "accumulated_data"

KAFKA_SERVER = "localhost:9092"


if __name__ == "__main__":
    bike_consumer = KafkaConsumer(bike_topic, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    bike_done = KafkaConsumer("bike_done", bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    # weather_consumer = KafkaConsumer(weather_topic, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    fuel_consumer = KafkaConsumer(fuel_topic, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    counted_bikes = 0
    history = []
    timestamp = 0
    timestamps = []

    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))


    while True:
        # publish merged data to kafka for grafana
        # for message in weather_consumer:  # update weather data object
        #     weather_data = json.loads(message.value.decode("utf-8"))
        messages = fuel_consumer.poll(timeout_ms=10)
        for msg in messages:  # update fuel data object
            for value in messages[msg]:
                fuel_data = json.loads(value.value)  

        messages = bike_consumer.poll(timeout_ms=200)
        for msg in messages:  # merge data with bike sensors
            for value in messages[msg]:
                data = json.loads(value.value)
                counted_bikes += data["count"]
                merge = MergedData(data, fuel_data)
                timestamp = data["time"]

                # json_data = json.dumps(merge)
                # future = producer.send(merged_data_topic, json_data)
                # future.get(timeout=10)

                # publish merged data to kafka
                summed_data = sum(fuel_data, counted_bikes)

                # json_data = json.dumps(summed_data)
                # future = producer.send(accumulated_data_topic, json_data)
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