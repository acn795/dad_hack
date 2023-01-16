#!/usr/bin/env python3.10

from dataclasses import dataclass
from datetime import datetime
from meteostat import Point, Hourly
from kafka import KafkaProducer
from kafka.producer.future import FutureRecordMetadata
import json
import time

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

@dataclass
class Weather:
    temp: float
    dwpt: float
    rhum: float
    prcp: float
    snow: float
    wdir: float
    wspd: float
    wpgt: float
    pres: float
    tsun: float
    coco: float

def load_batch_hourly(start_time):
    # Create Point for Hamburg, Germany
    hamburg = Point(53.5502, 9.9920, 6)

    # Get daily data from start_time
    data = Hourly(hamburg, start_time, datetime.now())
    data = data.fetch()

    # Get last value, because only publish per hour
    last = data.loc[data.last_valid_index()]
    
    weatherData = Weather(
        last["temp"],
        last["dwpt"],
        last["rhum"],
        last["prcp"],
        last["snow"],
        last["wdir"],
        last["wspd"],
        last["wpgt"],
        last["pres"],
        last["tsun"],
        last["coco"],
    )

    # print(weatherData)

    producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    future: FutureRecordMetadata = producer.send(config.WEATHER_TOPIC, weatherData.__dict__)
    future.get(timeout=10)

if __name__ == "__main__":
    while True:
        dt = datetime.now()

        load_batch_hourly(datetime(dt.year, dt.month, dt.day))

        time.sleep(60*60)
