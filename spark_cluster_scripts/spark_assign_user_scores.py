#assign genre scores to users cassandra table and then save table

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

#read scored releases table, create df
scored_releases_df=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="releases_scored", keyspace="discorecs")\
  .load()
scored_releases_df.count()
scored_releases_df

#read users table, create df
users_df=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="users", keyspace="discorecs")\
  .load()
users_df.count()
users_df

#create tables from dataframes for spark sql
sqlContext.registerDataFrameAsTable(users_df, "users")
sqlContext.registerDataFrameAsTable(scored_releases_df, "releases")

#collection_df schema for validation
collection_df_schema=StructType(List(StructField(release_id,IntegerType,true),StructField(blues_score,IntegerType,true),StructField(brass_score,IntegerType,true),StructField(childrens_score,IntegerType,true),StructField(classical_score,IntegerType,true),StructField(electronic_score,IntegerType,true),StructField(folk_score,IntegerType,true),StructField(funk_score,IntegerType,true),StructField(hiphop_score,IntegerType,true),StructField(jazz_score,IntegerType,true),StructField(latin_score,IntegerType,true),StructField(nonmusic_score,IntegerType,true),StructField(pop_score,IntegerType,true),StructField(reggae_score,IntegerType,true),StructField(rock_score,IntegerType,true),StructField(stagescreen_score,IntegerType,true)))

def sumScores(df)
        df.unionAll(
        df.select([
                F.lit('collection_scores').alias('release_id'),
                F.sum(df.blues_score).alias('blues_score'),
        ]))

collection_temp=sqlContext.sql("select user_id, collection, recommended_release, recommended_user from users")
collection_list=collection_temp.rdd.map(lambda x: x.collection).collect()
collection_list_users=collection_temp.rdd.map(lambda x: x.user_id).collect()

#remake query every loop, use i as iterator for each list of releases in collection_list
i=0
for list in collection_list:
	print("score assignment for ",i," element user started")
	local_user_id=collection_list_users[i].encode("ascii")
	collection_df=sqlContext.sql("select release_id, blues_score, brass_score, childrens_score, classical_score, electronic_score, folk_score, funk_score, hiphop_score, jazz_score, latin_score, nonmusic_score, pop_score, reggae_score, rock_score, stagescreen_score from releases where release_id like ''")
	collection_scores_df=collection_df.drop("release_id")
	for number in list:
		list_element=number
		print(list_element)
		query="select release_id, blues_score, brass_score, childrens_score, classical_score, electronic_score, folk_score, funk_score, hiphop_score, jazz_score, latin_score, nonmusic_score, pop_score, reggae_score, rock_score, stagescreen_score from releases where release_id like {}".format(list_element)
		collection_df=collection_df.union(sqlContext.sql(query))
	collection_df=collection_df.withColumn("user_id", lit(local_user_id))
	collection_scores_df=collection_df.groupBy('user_id').agg(sum('blues_score').alias('blues_score'), sum('brass_score').alias('brass_score'), sum('childrens_score').alias('childrens_score'), sum('classical_score').alias('classical_score'), sum('electronic_score').alias('electronic_score'), sum('folk_score').alias('folk_score'), sum('funk_score').alias('funk_score'), sum('hiphop_score').alias('hiphop_score'), sum('jazz_score').alias('jazz_score'), sum('latin_score').alias('latin_score'), sum('nonmusic_score').alias('nonmusic_score'), sum('pop_score').alias('pop_score'), sum('reggae_score').alias('reggae_score'), sum('rock_score').alias('rock_score'), sum('stagescreen_score').alias('stagescreen_score'))
	i += 1

#assign genre scores to dataframes for each genre
collection_temp=collection.join(collection_scores_df, collection_temp.user_id == collection_scores_df.user_id, 'left_outer')

#save collection_temp to cassandra
collection_temp.write\
	.format("org.apache.spark.sql.cassandra")\
	.mode("overwrite")\
	.options(table="users_scored", keyspace="discorecs", cluster="cassandra-cluster")\
	.save()
