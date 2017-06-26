# discorecs
a recommendation engine for vinyl junkies

# Description
Discorecs is based on real release metadata collected from Discogs data dumps and engineered data on user behavior. A large volume of users may be generated, along with their collections, for the purposes of recommendations. Release metadata is used to assign genre scores to users' collections, and users are subsequently assigned scores within each of Discogs' 15 genres. Users are ranked within each genre and a recommended friend and release are then generated for each user. Stream processing is used to update collections for users, and batch processing is used to generate recommendations. During tests, a volume of 10M users and 10K events/sec was sustained, and unique recommendations were generated for each user. A demo of discorecs is available at http://mmmckittrick.com/discorecs.

# Setup
1. Pegasus, a VM-based deployment tool published by Insight Data Science, is used to deploy the three clusters (Kafka, Spark, and Cassandra) used in this project. All the necessary scripts can be found in the deployment directory. View the DEPLOYMENT_README.md  for detailed instructions on configuring and deploying the three clusters. Note that an AWS account and PEM key are necessary to deploy clusters via this method.

2. Once the three clusters are spun up, an XML copy of a Discogs data dump will need to be used. For the purposes of the demo, I have made a copy of the June data dump available on S3 (https://console.aws.amazon.com/s3/buckets/discogs-recommender/dumps/), but if you wish to use a more current (or previous) data dump, Discogs maintains a repository here: http://data.discogs.com/. (If you wish to use a different data dump, modify the indicated line in spark_cluster_scripts/scala_parse_releases_xml.sh to point to the correct s3 or local path)

3. In the kafka_cluster_scripts directory, you will find all the necessary scripts to create the Kafka topics and start the producers. Access a node in the Kafka cluster and copy the kafka_cluster_scripts directory onto it. start_kafka_producers.sh will create the topics and start each producer, or the collection and activity producers may be started independently by running the commands included in the shell script. Each producer will run until canceled in the terminal.

4. Once the producers have started and are posting to their respective Kafka topics, create the Cassandra keyspace and tables on the Cassandra cluster by accessing a node in the Cassandra cluster and opening cqlsh. Paste the contents of discorecs_schema.cql
 into cqlsh to create the necessary tables.
 
5. The scripts in the spark_cluster_scripts directory are used in parallel, with Spark Streaming jobs as consumers for the Kafka topics and Spark batch jobs to populate the releases table, assign scores to releases and users, and generate recommendations. Prior to batch processing, scala_parse_releases_xml.sh must be executed to allow the metadata from the Discogs data dump releases XML to be imported to the Cassandra releases table.

6. At this point, each of the Spark batch processing scripts may be executed independently, in order to assign release scores to either releases or users, or to consume from the Kafka topics on new collections or user activity. Alternatively, executing spark_submits.sh will allow all of the batch processing scripts to run automatically in cron each hour, where spark_assign_user_scores will assign recommendations to each user as a final result. Note that if executing Spark scripts manually, spark_assign_release_scores must be executed prior to spark_assign_user_scores in order for release scores to propagate to the users table for ranking purposes.

# Results
