#!/usr/bin/env python3.10

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession \
          .builder \
          .appName("APP") \
          .getOrCreate()

    df = spark\
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mannnnnnno") \
        .option("startingOffsets", "earliest") \
        .load()
        

    query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .option("checkpointLocation", "path/to/HDFS/dir") \
        .start()

    query.awaitTermination()




#     spark = (
#         SparkSession.builder.appName("Kafka Pyspark Streaming Learning")
#         .master("local[*]")
#         .getOrCreate()
#     )
#     spark.sparkContext.setLogLevel("ERROR")
    
# KAFKA_TOPIC_NAME = "mannnnnnno"
# KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
# sampleDataframe = (
#         spark.readStream.format("kafka")
#         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
#         .option("subscribe", KAFKA_TOPIC_NAME)
#         .option("startingOffsets", "latest")
#         .load()
#     )

# base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
# base_df.printSchema()










# from pyspark.sql import SparkSession
# import json
# import time
# import numpy 
# import requests

# from dataclasses import dataclass
# from datetime import datetime
# from kafka import KafkaProducer, KafkaConsumer
# from kafka.producer.future import FutureRecordMetadata
# from datetime import timedelta

# KAFKA_SERVER = "localhost:9092"
# KAFKA_TOPIC = "mannnnnnno"
# consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_SERVER, value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# spark = SparkSession.builder.appName("test.de").getOrCreate()

# def get_data():
#     for message in consumer:
#         print(message.value)

# print(spark)
# rdd = spark.sparkContext.parallelize(get_data)
# print("RDD Count", rdd.count())

