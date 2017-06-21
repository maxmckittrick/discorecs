#consume from user collections topic and write to dstream
 
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
conf = SparkConf() \
  .setAppName("PySpark Kafka Consumer") \
  .set("spark.streaming.concurrentJobs",2) \
  .set("spark.streaming.kafka.maxRatePerPartition",5)

#set up contexts
sc = SparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 1) # 1 second window
brokers = ["10.0.0.8", "10.0.0.9", "10.0.0.13"]
topic = "user-collection-stream"
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder\
                                                                 .config(conf=sparkConf)\
                                                                 .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(rdd):
  #collectionsSchema = StructType().add("user_id", "string").add("collection", "array")
  spark=getSparkSessionInstance(rdd.context.getConf())
  collectionsDF=spark.read.json(rdd)
  #do stuff to DF here
  collectionsDF.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="users", keyspace="discorecs")\
    .save()

#parse kafka streams into python dicts
kvs = KafkaUtils.createDirectStream(stream, [topic], {"metadata.broker.list": "ec2-34-226-54-47.compute-1.amazonaws.com:9092"})
#kvs.pprint()
c = kvs.map(lambda x: x[1])
#collections = kvs.map(lambda x: json.loads(x[1]))
#kv = collections.map(lambda d: (d["user_id"], d["collection"]))
c.pprint()
c.foreachRDD(process)

#collections.pprint()

#store collections in dataframe

#collections.foreachRDD(process)

stream.start()
stream.awaitTermination()
