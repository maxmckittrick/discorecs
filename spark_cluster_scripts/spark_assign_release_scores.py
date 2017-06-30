#assign genre scores to releases cassandra table as a dataframe, then save dataframe as releases_scored table

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
  .setAppName("PySpark Release Genre Score Calculator")

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

#read releases table, create df
releases_df=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="releases", keyspace="discorecs")\
  .load()
releases_df=releases_df.withColumnRenamed(genre, genre.lower()) #lower-case genres for releases dataframe

#assign genre scores to dataframes for each genre
sqlContext.registerDataFrameAsTable(df, "releases") #register dataframe for spark sql operations
scores_df=releases_df.select('release_id', 'artist', 'genre', 'title')

#add columns for genre scores one at a time
with open('~/spark_cluster_scripts/genres.txt') as f:
  lines=f.read().splitlines()
for i in range(len(lines))
  temp_df=sqlContext.sql("select release_id, "+lines[i]+"_score from releases where genre like '%"+lines[i][:3]+"%'") #first three characters of genre string are sufficient to identify genre
  temp_df_scores=temp_df.withColumn(lines[i]+'_score', lit(1))
  temp_df_scores=temp_df_scores.select(col("release_id").alias("local_release_id"), col(lines[i]+"_score"))
  scores_df=scores_df.join(temp_df_scores, scores_df.release_id==temp_df_scores.local_release_id, 'left_outer')
  scores_df=scores_df.drop(('local_release_id')

#save scores_df to cassandra
scores_df.write\
  .format("org.apache.spark.sql.cassandra")\
  .mode("overwrite")\
  .options(table="releases_scored", keyspace="discorecs", cluster="cassandra-cluster")\
  .save()
