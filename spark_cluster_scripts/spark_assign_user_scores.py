#assign genre scores to users in staging table, insert into users_scored table, empty users_staging, then rank all users and assign recommendations

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
conf=SparkConf()\
  .setAppName("PySpark User Ranks & Recommendations Generator")

#create SparkSession
def getSparkSessionInstance(sparkConf):
  if ("sparkSessionSingletonInstance" not in globals()):
    globals()["sparkSessionSingletonInstance"]=SparkSession.builder\
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

#read users staging table, create df
users_staging_df=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="users_staging", keyspace="discorecs")\
  .load()

#create tables and schema from dataframes for spark sql
sqlContext.registerDataFrameAsTable(users_staging_df, "users_staging")
users_schema=users_staging_df.schema
sqlContext.registerDataFrameAsTable(scored_releases_df, "releases")
releases_schema=scored_releases_df.schema

#map nested lists for users' collections in staging table
collection_staging=sqlContext.sql("select user_id, collection, recommended_release, recommended_user from users_staging") #all users in users_staging will have existing recommended release/user, if any, removed
collection_list=collection_staging.rdd.map(lambda x: x.collection)
collection_list_users=collection_staging.rdd.map(lambda x: x.user_id)

#remake query every loop, iterate for each list of releases in collection_list
i=0
query_content="select release_id, blues_score, brass_score, childrens_score, classical_score, electronic_score, folk_score, funk_score, hiphop_score, jazz_score, latin_score, nonmusic_score, pop_score, reggae_score, rock_score, stagescreen_score from releases where release_id like "
for list in collection_list:
  local_user_id=collection_list_users[i].encode("ascii") #users may have non-utf8 names
  collection_df=sqlContext.sql(query_content+"''") #empty collection_df for each user
  collection_scores_df=collection_df.drop("release_id") #will be re-added after iteration
  for number in list:
    list_element=number
    query=query_content+"{}".format(list_element)
    collection_df=collection_df.union(sqlContext.sql(query))
  collection_df=collection_df.withColumn("user_id", lit(local_user_id)) #add user IDs back to collection_df
  collection_scores_df=collection_df.groupBy('user_id').agg(sum('blues_score').alias('blues_score'), sum('brass_score').alias('brass_score'), sum('childrens_score').alias('childrens_score'), sum('classical_score').alias('classical_score'), sum('electronic_score').alias('electronic_score'), sum('folk_score').alias('folk_score'), sum('funk_score').alias('funk_score'), sum('hiphop_score').alias('hiphop_score'), sum('jazz_score').alias('jazz_score'), sum('latin_score').alias('latin_score'), sum('nonmusic_score').alias('nonmusic_score'), sum('pop_score').alias('pop_score'), sum('reggae_score').alias('reggae_score'), sum('rock_score').alias('rock_score'), sum('stagescreen_score').alias('stagescreen_score'))
  i+=1 #iterate for each user in staging table

#assign genre scores to collection_staging table
collection_staging=collection_staging.join(collection_scores_df, collection_staging.user_id==collection_scores_df.user_id, 'left_outer')

#save collection_staging to cassandra
collection_staging.write\
  .format("org.apache.spark.sql.cassandra")\
  .mode("append")\
  .options(table="users_scored", keyspace="discorecs", cluster="cassandra-cluster")\
  .save()

#use users_staging_df schema to write empty dataframe to users_staging table
users_empty=sqlContext.createDataFrame(sc.emptyRDD(), users_schema)
users_empty.write\
  .format("org.apache.spark.sql.cassandra")\
  .mode("overwrite")\
  .options(table="users_staging", keyspace="discorecs", cluster="cassandra-cluster")\
  .save()

#construct df of all users from users_scored
users_scored_df=spark.read\
  .format("org.apache.spark.sql.cassandra")\
  .options(table="users_scored", keyspace="discorecs")\
  .load()

#table of all users, one copy of table with users ranked in each genre
sqlContext.registerDataFrameAsTable(users_scored_df, "users_scored")
with open('~/spark_cluster_scripts/genres.txt') as f:
  lines=f.read().splitlines()
for i in range(len(lines))
  lines[i]_ranked_df=sqlContext.sql("select user_id, "+lines[i]+"_score, recommended_release, recommended_user from users_scored order by "+lines[i]+"_score desc")

#split users_scored_df
old_users_df=sqlContext.sql("select * from users_scored where recommended_user is not null")
new_users_df=sqlContext.sql("select * from users_scored where recommended_user is null")

#assign recommended users and releases
def user_recommendations(row):
  highest_rank=new_users_df.row1["max(blues_score, brass_score, childrens_score, classical_score, electronic_score, folk_score, funk_score, hiphop_score, jazz_score, latin_score, nonmusic_score, pop_score, reggae_score, rock_score, stagescreen_score)"] #identify favorite genre
  highest_rank_value=users+scored_df.select(highest_rank+"_score from users_scored where user_id like "+row.user_id)
  collections=.select("collection from users_scored") #used for recommended release aggregation
  tiebreak_rank=new_users_df.row2["max(blues_score, brass_score, childrens_score, classical_score, electronic_score, folk_score, funk_score, hiphop_score, jazz_score, latin_score, nonmusic_score, pop_score, reggae_score, rock_score, stagescreen_score)"] #identify 2nd favorite genre
  recommended_user_index_range=[highest_rank]_ranked_df.select("user_id from users_scored where "+highest_rank+"_score > "+highest_rank_value)
  recommended_user=[tiebreak_rank]_ranked_df.row1["max("+tiebreak_rank+"_score)"]
  recommended_release_df=new_users_df.join(highest_rank" from users_scored", col("collection")==col("collection"))
  recommended_release=recommended_release_df.groupby("collection").row1[] #group by items in all collections, select first item
  return (row.recommended_user, row.recommended_release)
for row in new_users_df.rdd.collect()
  user_recommendations(row)

#combine new and old user tables, write to cassandra
users_combined_df=old_users_df.unionAll(new_users_df)
users_combined_df.write\
  .format("org.apache.spark.sql.cassandra")\
  .mode("overwrite")\
  .options(table="users_scored", keyspace="discorecs", cluster="cassandra-cluster")\
  .save()
