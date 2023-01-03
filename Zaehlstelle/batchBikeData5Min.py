import requests
import json
import time
from dataclasses import dataclass
from kafka import KafkaProducer
from datetime import datetime

@dataclass
class BikeSensor5Min:
    ts: str
    id: int
    name: str
    count: int
    time: str
    coordinateType: str
    coordinates: list

# class BikeSensor5Min:

def get_batch_data_5_min():
    # while True:
    resp = requests.get("https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName%20eq%20%27HH_STA_HamburgerRadzaehlnetz%27%20and%20Datastreams/properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlfeld_5-Min%27&$count=true&$expand=Datastreams($filter=properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlfeld_5-Min%27;$expand=Observations($orderby=phenomenonTime%20desc))")
    # resp = requests.get("https://iot.hamburg.de/v1.1/Things?$filter=Datastreams/properties/serviceName%20eq%20%27HH_STA_HamburgerRadzaehlnetz%27%20and%20Datastreams/properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlstelle_15-Min%27&$count=true&$expand=Datastreams($filter=properties/layerName%20eq%20%27Anzahl_Fahrraeder_Zaehlstelle_15-Min%27;$expand=Observations($top=10;$orderby=phenomenonTime%20desc))")
    response = json.loads(resp.content)
    done = False
    cnt = 0
    sensors = []
    phenomenaTime = ""
    epoch_time = datetime(1970, 1, 1)
    while(not done):
        if resp.status_code == 200:
            for feature in response['value']:
                for stream in feature['Datastreams']:
                    batch_link = 'https://iot.hamburg.de/v1.1/Datastreams(' + str(stream['@iot.id']) + ')/Observations?$skip=0&$orderby=phenomenonTime+desc'
                
                    f = open("safestate.txt", "r")
                    if str(stream['@iot.id']) in f.read():
                        print("skip")
                        continue

                    batch_resp = requests.get(batch_link, timeout=30)
                    batch_response = json.loads(batch_resp.content)
                    
                    batch_done = False
                    if batch_resp.status_code == 200:
                        while not batch_done:
                            for result_value in batch_response['value']:
                                date = datetime.strptime(result_value['phenomenonTime'].split('/')[0].split('Z')[0], "%Y-%m-%dT%H:%M:%S.%f")
                                bikesensor = BikeSensor5Min((date - epoch_time).total_seconds, feature['@iot.id'], feature['name'], result_value['result'], result_value['phenomenonTime'], stream['observedArea']['type'], stream['observedArea']['coordinates'])
                                sensors.append(bikesensor)
                                date = datetime.strptime(result_value['phenomenonTime'].split('/')[0].split('Z')[0], "%Y-%m-%dT%H:%M:%S.%f")
                                cnt += 1
                            if cnt %  10000 == 0:
                                print(cnt)
                            if '@iot.nextLink' not in batch_response:
                                batch_done = True
                            else:
                                try:
                                    batch_done = True
                                    batch_resp = requests.get(batch_response['@iot.nextLink'], timeout=30)
                                except requests.exceptions.Timeout:
                                    print("TIMEOUT")

                                batch_response = json.loads(batch_resp.content)
                    else:
                        print("BATCHREQUEST NOT OK")

                    data_dict = [sensor.__dict__ for sensor in sensors]
                    sensors = []
                    # print(data_dict)
                    producer = KafkaProducer(bootstrap_servers='localhost:9092')
                    topic = 'bikes_5_min'
                    
                    for bike_data in data_dict:
                        # pass
                        # send every hour data as single message to kafka
                        json_data = json.dumps(bike_data)
                        # print(json_data)
                        future = producer.send(topic, bike_data)

                    f = open("safestate.txt", "a")
                    f.write(str(stream['@iot.id']) + " \n")
                    f.close()
        else:
            print("REQUEST NOT OK")
        if '@iot.nextLink' not in response:
            done = True
        else:
            done = True
            resp = requests.get(response['@iot.nextLink'])
            response = json.loads(resp.content)

    print("cnt ",cnt)
    # data_dict = [sensor.__dict__ for sensor in sensors]
    # # print(data_dict)
    # producer = KafkaProducer(bootstrap_servers='localhost:9092')
    # topic = 'bikes_5_min'
    
    # for bike_data in data_dict:
    #     # pass
    #     # send every hour data as single message to kafka
    #     json_data = json.dumps(bike_data)
    #     # print(json_data)
    #     future = producer.send(topic, bike_data)

if __name__ == "__main__":
    get_batch_data_5_min()
        # time.sleep(60 * 5)