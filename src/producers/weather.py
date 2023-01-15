#!/usr/bin/env python3.10

import numpy 

from datetime import datetime
from meteostat import Point, Hourly
from kafka import KafkaProducer
import json

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

def load_batch_hourly(start_time):
    # Set time period

    end = datetime.now()
    # Create Point for Hamburg, Germany
    hamburg = Point(53.5502, 9.9920, 6)

    # Get daily data for 2018
    data = Hourly(hamburg, start_time, end)
    data = data.fetch()
    
    data_dict = data.to_dict(orient='records')

    producer = KafkaProducer(bootstrap_servers=config.KAFKA_SERVER)
    topic = config.WEATHER_TOPIC
    for hour_data in data_dict:
        # send every hour data as single message to kafka
        json_hour_data = json.dumps(hour_data)
        print(json_hour_data)
#        future = producer.send(topic, json_hour_data)


if __name__ == "__main__":
    load_batch_hourly(datetime(2020, 1, 1))
