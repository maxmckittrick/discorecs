#consume from user collections topic and publish to cassandra

import pyspark_cassandra
import pyspark_cassandra.streaming
import json
from pyspark_cassandra import CassandraSparkContext
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

spark-submit --conf spark.cassandra.connection.host=10.0.0.18 --packages dibbhatt:kafka-spark-consumer:1.0.10 com.datastax.spark:spark-cassandra-connector_2.11:2.0.1

# set up our contexts
sc = CassandraSparkContext(conf=conf)
sql = SQLContext(sc)
stream = StreamingContext(sc, 1) # 1 second window

kafka_stream = KafkaUtils.createStream(stream, \
                                       "10.0.0.9:9092", \ #figure out cassandra port
                                       "raw-event-streaming-consumer",
                                        {"user_id":1})

#parse kafka stream into python dicts
parsed = kafka_stream.map(lambda (k, v): json.loads(v))
summed = parsed.map(lambda event: (event['user_id'], 1)).\
                reduceByKey(lambda x,y: x + y).\
                map(lambda x: {"user_id": x[0], "collection": x[1]})
summed.pprint()
