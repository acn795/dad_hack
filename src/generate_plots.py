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
from merge_data import AccumulatedData

if __name__ == "__main__":
    # Consumer
    consumer = KafkaConsumer(config.ACCUMULATED_DATA_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    # Plot data
    history = []
    timestamp = 0
    timestamps = []

    while True:
        messages = consumer.poll(timeout_ms=10)
        for msg in messages:  # update fuel data object
            for value in messages[msg]:
                print(value.value)
                # weather_data = Weather(
                #     value.value["temp"],
                #     value.value["dwpt"],
                #     value.value["rhum"],
                #     value.value["prcp"],
                #     value.value["snow"],
                #     value.value["wdir"],
                #     value.value["wspd"],
                #     value.value["wpgt"],
                #     value.value["pres"],
                #     value.value["tsun"],
                #     value.value["coco"],
                # )
                
        # messages = bike_done_consumer.poll(timeout_ms=10)
        # for message in messages:  # update fuel data object
        #     history.append(counted_bikes)
        #     timestamps.append(timestamp)
        #     counted_bikes = 0
        #     timestamp = 0
        #     # plt.plot(history)
        #     plt.plot(timestamps, history)
        #     plt.xticks(np.arange(len(timestamps)), timestamps, rotation=45, ha='right')
        #     plt.xlabel('Counted Bikes accumulated')
        #     plt.ylabel('Time 5 Min Interval')
        #     plt.title('Gezählte Fahrräder')
        #     # plt.ion()
        #     plt.show()