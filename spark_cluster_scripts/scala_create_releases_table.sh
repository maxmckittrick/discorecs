#create cassandra table with spark shell (scala) from XML dump of master releases, see spark_submits file for spark shell config

val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "release").load("s3a://discogs-recommender/dumps/discogs_20170601_releases.xml")

val explodeDF = df.withColumn("artist", df("artists.artist.name"))

val explodeDF2 = explodeDF.withColumn("genre", df("genres.genre"))

val explodeDF3 = explodeDF2.withColumn("release_id", df("_id"))

val selectedData = explodeDF3.select($"release_id",$"title",$"artist",$"genre")

selectedData.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "releases", "keyspace" -> "discorecs", "cluster" -> "cassandra-cluster")).save()
