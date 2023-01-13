import requests
import json
import time
from dataclasses import dataclass
from kafka import KafkaProducer
import datetime

@dataclass
class BikeSensor5Min:
    id: int
    name: str
    count: int
    time: str
    coordinateType: str
    coordinates: list

# class BikeSensor5Min:

def get_data_5_min():
    # while True:
    resp = requests.get("https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName%20eq%20%27HH_STA_HamburgerRadzaehlnetz%27%20and%20Datastreams/properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlfeld_5-Min%27&$count=true&$expand=Datastreams($filter=properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlfeld_5-Min%27;$expand=Observations($top=10;$orderby=phenomenonTime%20desc))")
    # resp = requests.get("https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName%20eq%20%27HH_STA_HamburgerRadzaehlnetz%27%20and%20Datastreams/properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlstelle_15-Min%27&$count=true&$expand=Datastreams($filter=properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlstelle_15-Min%27;$expand=Observations($top=10;$orderby=phenomenonTime%20desc))")
    response = json.loads(resp.content)
    done = False
    cnt = 0
    sensors = []
    phenomenaTime = ""
    while(not done):
        if resp.status_code == 200:
            for feature in response['value']:
                for stream in feature['Datastreams']:
                    date = stream['Observations'][0]['phenomenonTime'].split('T')[0]
                    if date == datetime.datetime.now().strftime("%Y-%m-%d"):
                        cnt += 1
                        if len(stream['Observations']) > 0:
                            # print(stream['description'])
                            # print(stream['Observations'][0]['result'])
                            # print(stream['Observations'][0]['phenomenonTime'])
                            bikesensor = BikeSensor5Min(feature['@iot.id'], feature['name'], stream['Observations'][0]['result'], stream['Observations'][0]['phenomenonTime'], stream['observedArea']['type'], stream['observedArea']['coordinates'])
                            sensors.append(bikesensor)
                            phenomenaTime = stream['Observations'][0]['phenomenonTime']
                        else: # sometimes no data fetched
                            # print(stream['description'])
                            # print(0)
                            # print(phenomenaTime)
                            bikesensor = BikeSensor5Min(feature['@iot.id'], feature['name'], 0, phenomenaTime, stream['observedArea']['type'], stream['observedArea']['coordinates'])
                            sensors.append(bikesensor)
        else:
            print("REQUEST NOT OK")
        if '@iot.nextLink' not in response:
            done = True
        else:
            resp = requests.get(response['@iot.nextLink'])
            response = json.loads(resp.content)

    # print("cnt ",cnt)
    data_dict = [sensor.__dict__ for sensor in sensors]
    # print(data_dict)
    # producer = KafkaProducer(bootstrap_servers='localhost:9092')
    KAFKA_SERVER = "localhost:9092"
    producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    topic = 'bike_data_5_min'

    print("publish data")
    
    for bike_data in data_dict:
        # send every hour data as single message to kafka
        json_data = json.dumps(bike_data)
        future = producer.send(topic, json_data)
        future.get(timeout=10)

    time.sleep(2)
    future = producer.send("bike_done", json_data)
    future.get(timeout=10)


    print("data published")

if __name__ == "__main__":
    while(True):
        starttime = time.time()
        get_data_5_min()
        delta = time.time() - starttime
        time.sleep(60 * 0.5 - delta)