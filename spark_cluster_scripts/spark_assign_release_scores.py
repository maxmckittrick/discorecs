#assign genre scores to releases cassandra table and then save table

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

#create conf
conf = SparkConf()\
        .setAppName("PySpark Release Genre Score Calculator")

#create SparkSession
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession.builder\
                                                                 .config(conf=sparkConf)\
                                                                 .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

#set up SQLContext
sc=SparkContext(conf=conf)
sqlContext=SQLContext(sc)
spark=getSparkSessionInstance(sc.getConf())

#read releases table, create df
releases_df=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="releases", keyspace="discorecs")\
  .load()
releases_df.count()
releases_df

#assign genre scores to dataframes for each genre
sqlContext.registerDataFrameAsTable(df, "releases")
scores_df=releases_df.select('release_id', 'artist', 'genre', 'title')

temp_df=sqlContext.sql("select release_id, blues_score from releases where genre like '%Blues%'")
temp_df_scores = temp_df.withColumn('blues_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("blues_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, brass_score from releases where genre like '%Brass%'")
temp_df_scores = temp_df.withColumn('brass_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("brass_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, childrens_score from releases where genre like '%Children%'")
temp_df_scores = temp_df.withColumn('childrens_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("childrens_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, classical_score from releases where genre like '%Classical%'")
temp_df_scores = temp_df.withColumn('classical_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("classical_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, electronic_score from releases where genre like '%Electronic%'")
temp_df_scores = temp_df.withColumn('electronic_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("electronic_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, folk_score from releases where genre like '%Folk%'")
temp_df_scores = temp_df.withColumn('folk_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("folk_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, funk_score from releases where genre like '%Funk%'")
temp_df_scores = temp_df.withColumn('funk_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("funk_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, hiphop_score from releases where genre like '%Hip%'")
temp_df_scores = temp_df.withColumn('hiphop_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("hiphop_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, jazz_score from releases where genre like '%Jazz%'")
temp_df_scores = temp_df.withColumn('jazz_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("jazz_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, latin_score from releases where genre like '%Latin%'")
temp_df_scores = temp_df.withColumn('latin_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("latin_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, nonmusic_score from releases where genre like '%Non%'")
temp_df_scores = temp_df.withColumn('nonmusic_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("nonmusic_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, pop_score from releases where genre like '%Pop%'")
temp_df_scores = temp_df.withColumn('pop_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("pop_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, reggae_score from releases where genre like '%Reggae%'")
temp_df_scores = temp_df.withColumn('reggae_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("reggae_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, rock_score from releases where genre like '%Rock%'")
temp_df_scores = temp_df.withColumn('rock_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("rock_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

temp_df=sqlContext.sql("select release_id, stagescreen_score from releases where genre like '%Stage%'")
temp_df_scores = temp_df.withColumn('stagescreen_score', lit(1))
temp_df_scores = temp_df_scores.select(col("release_id").alias("local_release_id"), col("stagescreen_score"))
scores_df=scores_df.join(temp_df_scores, df.release_id == temp_df_scores.local_release_id, 'left_outer')
scores_df=scores_df.drop('local_release_id')

#scores_df.persist()
scores_df.count()
scores_df

#save scores_df to cassandra
scores_df.write\
	.format("org.apache.spark.sql.cassandra")\
	.mode("overwrite")\
	.options(table="releases_scored", keyspace="discorecs", cluster="cassandra-cluster")\
	.save()
