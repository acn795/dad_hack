#!/usr/bin/env python3


from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

future = producer.send('example_topic', b'Hallo Welt!')
result = future.get(timeout=60)
print(result)
