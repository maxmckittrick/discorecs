#consume from user activity topic and write to dstream, send results to users_staging table

import pyspark
import json
from pyspark.sql import SQLContext
from functools import partial
from itertools import combinations
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql import Row, SparkSession

#create conf
conf=SparkConf() \
  .setAppName("PySpark Activity Kafka Consumer") \
  .set("spark.streaming.concurrentJobs",2) \ #allow both consumers to run simultaneously
  .set("spark.streaming.kafka.maxRatePerPartition",5)

#set up contexts
sc=SparkContext(conf=conf)
sql=SQLContext(sc)
stream=StreamingContext(sc, 1) #1 second window

#kafka brokers should be listed one private IP (e.g.; 10.0.0.1) on each line in ~/config/brokers.txt, one public DNS with port, default 9092 (e.g.; ec2-1-2-3-4.compute-1.amazonaws.com:9092) on each line  in ~/config/brokers_dns.txt
with open('~/config/brokers.txt') as f:
  brokers=f.read().splitlines()
topic = "user_activity_stream"
with open('~/config/brokers_dns.txt') as f2:
  brokers_dns=f2.read().splitlines()
brokers_dns_str=", ".join(repr(e) for e in brokers_dns) #necessary for kafkautils

#create SparkSession in case it doesn't exist
def getSparkSessionInstance(sparkConf):
  if ("sparkSessionSingletonInstance" not in globals()):
    globals()["sparkSessionSingletonInstance"]=SparkSession.builder\
      .config(conf=sparkConf)\
      .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):
  spark=getSparkSessionInstance(rdd.context.getConf())
  collectionsDF=spark.read.json(rdd)
  collectionsDF.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="users_staging", keyspace="discorecs")\
    .save()

#parse kafka streams into python dicts
kvs=KafkaUtils.createDirectStream(stream, [topic], {"metadata.broker.list": brokers_dns_str})
cols=kvs.map(lambda x: x[1]) #map each element in dict to kvs
cols.foreachRDD(process)

#stream will continue until terminated
stream.start()
stream.awaitTermination()
