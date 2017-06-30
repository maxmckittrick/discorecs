#test users_scored table and display stats on users and recommendations

import csv
import cStringIO
import pprint
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
from pyspark.sql.functions import *

#create conf for pyspark
conf=SparkConf()\
  .setAppName("PySpark Recommendation Stats")

#create SparkSession in case it doesn't exist
def getSparkSessionInstance(sparkConf):
  if ("sparkSessionSingletonInstance" not in globals()):
    globals()["sparkSessionSingletonInstance"]=SparkSession.builder\
      .config(conf=sparkConf)\
      .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

#set up SQLContext for spark sql
sc=SparkContext(conf=conf)
sqlContext=SQLContext(sc)
spark=getSparkSessionInstance(sc.getConf())

#read users_scored table, create df
users_scored_df=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="users_scored", keyspace="discorecs")\
  .load()

#print stats for dataframe
users_scored_df.count().show()
users_scored_df.filter(size(col("collection"))).agg({"collection"}; {"avg"}).show()
users_scored_df.select("recommended_user").distinct().rdd.map(lambda r: r[0]).collect().show()
users_scored_df.select("recommended_release").distinct().rdd.map(lambda r: r[0]).collect().show()
