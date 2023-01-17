#!/usr/bin/env python3.10

import json
import os

from dataclasses import dataclass
from datetime import datetime
from kafka import KafkaConsumer
from datetime import timedelta
import matplotlib.pyplot as plt

import config
from producers.fuel_price import FuelPrice
from producers.weather import Weather
from merge_data import AccumulatedData                

def plot(x_data, y_data, title: str, x_label: str, y_label: str, file_name: str):
    plt.clf()
    plt.plot(x_data, y_data)
    # plt.xticks(np.arange(len(history.keys())), history.keys(), rotation=45, ha='right')
    plt.xticks(rotation=45, ha='right')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    # plt.ion()
    # plt.show()
    home_path = os.path.expanduser('~')#
    path = f'{home_path}/dad_hack/monitoring/src/assets/{file_name}'
    print(path)
    plt.savefig(path)

if __name__ == "__main__":
    # Consumer
    consumer = KafkaConsumer(config.ACCUMULATED_DATA_TOPIC, bootstrap_servers=config.KAFKA_SERVER, value_deserializer=lambda m: m.decode('utf-8'))

    # Plot data
    history: dict[str, AccumulatedData] = {}

    while True:
        messages = consumer.poll(timeout_ms=10)
        for msg in messages:  # update fuel data object
            for value in messages[msg]:
                print(value.value)
                dict = json.loads(value.value)
                accumulated_data = AccumulatedData(
                    dict["timestamp"],
                    dict["bikes"],                    
                    FuelPrice(
                        dict["fuel_price"]["date"],
                        dict["fuel_price"]["e5"],
                        dict["fuel_price"]["e10"],
                        dict["fuel_price"]["diesel"],
                    ),
                    Weather(
                        dict["weather_data"]["temp"],
                        dict["weather_data"]["dwpt"],
                        dict["weather_data"]["rhum"],
                        dict["weather_data"]["prcp"],
                        dict["weather_data"]["snow"],
                        dict["weather_data"]["wdir"],
                        dict["weather_data"]["wspd"],
                        dict["weather_data"]["wpgt"],
                        dict["weather_data"]["pres"],
                        dict["weather_data"]["tsun"],
                        dict["weather_data"]["coco"],
                    ),
                )

                # history[accumulated_data.timestamp] = accumulated_data
                history[str(datetime.now().timestamp())] = accumulated_data
                
                # Base Data

                plot(
                    history.keys(),
                    [a.bikes for a in history.values()],
                    'Counted bikes accumulated',
                    'Time 5 Min Interval',
                    'Counted Bikes accumulated',
                    "counted_bikes.png"
                )

                # TODO SHOW ALL AND NOT ONLY RAIN
                plot(
                    history.keys(),
                    [a.weather_data.prcp for a in history.values()],
                    'Weather',
                    'Time 5 Min Interval',
                    'Weather condition',
                    "weather.png"
                )

                # TODO SHOW ALL AND NOT ONLY DIESEL
                plot(
                    history.keys(),
                    [a.fuel_price.diesel for a in history.values()],
                    'Fuel Price',
                    'Time 5 Min Interval',
                    'Fuel Price',
                    "fueal_price.png"
                )

                # Fuel Price Correlation

                plot(
                    history.keys(),
                    [a.fuel_price.diesel for a in history.values()],
                    'Diesel - Bike Count',
                    'Time 5 Min Interval',
                    'diesel in l / €',
                    "correlation_bike_vs_diesel.png"
                )

                plot(
                    history.keys(),
                    [a.fuel_price.e5 for a in history.values()],
                    'E5 - Bike Count',
                    'Time 5 Min Interval',
                    'e5 in l/ €',
                    "correlation_bike_vs_e5.png"
                )

                plot(
                    history.keys(),
                    [a.fuel_price.e10 for a in history.values()],
                    'E10 - Bike Count',
                    'Time 5 Min Interval',
                    'e10 in l / €',
                    "correlation_bike_vs_e10.png"
                )

                # Weather Correlation

                plot(
                    history.keys(),
                    [a.fuel_price.e10 for a in history.values()],
                    'Rain - Bike Count',
                    'Time 5 Min Interval',
                    'ml / m²',
                    "correlation_bike_vs_rain.png"
                )

                plot(
                    history.keys(),
                    [a.fuel_price.e10 for a in history.values()],
                    'Wind - Bike Count',
                    'Time 5 Min Interval',
                    'km/h',
                    "correlation_bike_vs_wind.png"
                )

                plot(
                    history.keys(),
                    [a.fuel_price.e10 for a in history.values()],
                    'Temp - Bike Count',
                    'Time 5 Min Interval',
                    '°C',
                    "correlation_bike_vs_temperature.png"
                )

