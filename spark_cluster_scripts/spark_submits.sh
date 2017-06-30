#this script expects the following files in ~/config:
#spark_master.txt, one line of the ivp4 of the master node in your spark cluster e.g.; 1.2.3.4
#cassandra_workers.txt, one line with each of the public DNS of nodes in your cassandra cluster separated by a comma with no space e.g.; ec2-1-2-3-4.compute-1.amazonaws.com,ec2-5-6-7-8.compute-1.amazonaws.com...

$SPARK_MASTER=$(<~/spark_master.txt)
$CASSANDRA_WORKERS=$(<~/cassandra_workers.txt)

#spark submit for parsing releases xml with spark (scala, not pyspark)
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages com.databricks:spark-xml_2.10:0.4.1,datastax:spark-cassandra-connector:2.0.0-s_2.11 scala_parse_releases_xml.sh

#spark submit for consuming user collections from kafka producer
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1,datastax:spark-cassandra-connector:2.0.0-s_2.11 spark_collections_consumer.py

#spark submit for consuming user activity from kafka producer
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1,datastax:spark-cassandra-connector:2.0.0-s_2.11 spark_activity_consumer.py

#spark submit for assigning genre scores to releases
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages datastax:spark-cassandra-connector:2.0.0-s_2.11 spark_assign_release_scores.py

#spark submit for assigning genre scores to users
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages datastax:spark-cassandra-connector:2.0.0-s_2.11 spark_assign_user_scores.py

#spark shell with spark xml and csv packages (for testing)
spark-shell --master spark://$SPARK_MASTER:7077 --conf spark.cassandra.connection.host=$CASSANDRA_WORKERS --packages com.databricks:spark-xml_2.10:0.4.1,com.databricks:spark-csv_2.10:1.5.0

#spark submit for parsing releases xml to csv with pyspark (unstable, you've been warned)
$SPARK_HOME/bin/spark-submit --master spark://$SPARK_MASTER:7077 --packages com.databricks:spark-xml_2.11:0.4.1 spark_parse_releases_xml.py

#crontab for spark job to automatically generate recommendations (run hourly), insert with crontab -e
@hourly ~/spark_cluster_scripts/spark_submits.sh
