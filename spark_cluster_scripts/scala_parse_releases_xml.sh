#create cassandra table with spark (scala) from XML dump of master releases, see spark_submits file for spark config

#!/usr/bin/env scala

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "release").load("s3a://discogs-recommender/dumps/discogs_20170601_releases.xml") #most recent releases dump as of june 2017

val explodeDF = df.withColumn("artist", df("artists.artist.name"))

val explodeDF2 = explodeDF.withColumn("genre", df("genres.genre"))

val explodeDF3 = explodeDF2.withColumn("release_id", df("_id"))

val selectedData = explodeDF3.select($"release_id",$"title",$"artist",$"genre") #the only necessary columns for the releases table are release_id and genre, title and artist are used for front-end purposes

selectedData.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "releases", "keyspace" -> "discorecs", "cluster" -> "cassandra-cluster")).save()
