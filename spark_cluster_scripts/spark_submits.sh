#spark submit for consuming user collections from kafka producer
$SPARK_HOME/bin/spark-submit --master spark://52.21.109.166:7077 --conf spark.cassandra.connection.host=ec2-34-224-164-96.compute-1.amazonaws.com,ec2-34-226-185-119.compute-1.amazonaws.com,ec2-52-7-155-249.compute-1.amazonaws.com,ec2-34-197-78-240.compute-1.amazonaws.com,ec2-34-198-157-157.compute-1.amazonaws.com --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1,datastax:spark-cassandra-connector:2.0.0-s_2.11 collections_spark_consumer.py

#spark submit for assigning genre scores to releases
$SPARK_HOME/bin/spark-submit --master spark://52.21.109.166:7077 --conf spark.cassandra.connection.host=ec2-34-224-164-96.compute-1.amazonaws.com,ec2-34-226-185-119.compute-1.amazonaws.com,ec2-52-7-155-249.compute-1.amazonaws.com,ec2-34-197-78-240.compute-1.amazonaws.com,ec2-34-198-157-157.compute-1.amazonaws.com --packages datastax:spark-cassandra-connector:2.0.0-s_2.11 spark_assign_release_scores.py

#spark submit for assigning genre scores to users
$SPARK_HOME/bin/spark-submit --master spark://52.21.109.166:7077 --conf spark.cassandra.connection.host=ec2-34-224-164-96.compute-1.amazonaws.com,ec2-34-226-185-119.compute-1.amazonaws.com,ec2-52-7-155-249.compute-1.amazonaws.com,ec2-34-197-78-240.compute-1.amazonaws.com,ec2-34-198-157-157.compute-1.amazonaws.com --packages datastax:spark-cassandra-connector:2.0.0-s_2.11 spark_assign_user_scores.py

#spark submit for parsing releases xml with pyspark (unstable, you've been warned)
$SPARK_HOME/bin/spark-submit --master spark://52.21.109.166:7077 --packages com.databricks:spark-xml_2.11:0.4.1 spark_parse_releases_xml.py

#spark submit for parsing releases xml with spark shell (scala), paste output from scala_... to execute
$SPARK_HOME/bin/spark-shell --master spark://52.21.109.166:7077 --conf spark.cassandra.connection.host=ec2-34-224-164-96.compute-1.amazonaws.com,ec2-34-226-185-119.compute-1.amazonaws.com,ec2-52-7-155-249.compute-1.amazonaws.com,ec2-34-197-78-240.compute-1.amazonaws.com,ec2-34-198-157-157.compute-1.amazonaws.com --packages com.databricks:spark-xml_2.10:0.4.1,datastax:spark-cassandra-connector:2.0.0-s_2.11

#spark shell with spark xml and csv packages
spark-shell --master spark://52.21.109.166:7077 --packages com.databricks:spark-xml_2.10:0.4.1,com.databricks:spark-csv_2.10:1.5.0
