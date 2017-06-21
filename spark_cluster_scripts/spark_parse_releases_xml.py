#parse releases to csv using spark (deprecated, don't use this unless you need full inheritance from xml structs)

import pyspark
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

#create conf
conf = SparkConf()\
	.setAppName("Spark XML-CSV Converter")\

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.xml').options(rowTag='release').load('s3a://discogs-recommender/dumps/discogs_releases_sample.xml')
df.select("_id", "artists", "title", "genres").write.format('com.databricks.spark.csv').options(header='TRUE').save('discogs_releases_parsed.csv')
