# discorecs
a recommendation engine for vinyl junkies

# Description
Discorecs is based on real release metadata collected from Discogs data dumps and engineered data on user behavior. A large volume of users may be generated, along with their collections, for the purposes of recommendations. Release metadata is used to assign genre scores to users' collections, and users are subsequently assigned scores within each of Discogs' 15 genres. Users are ranked within each genre and a recommended friend and release are then generated for each user. Stream processing is used to update collections for users, and batch processing is used to generate recommendations. During tests, a volume of 10M users and 10K events/sec was sustained, and unique recommendations were generated for each user. A demo of discorecs is available at http://mmmckittrick.com/discorecs.

# Setup
1. Pegasus, a VM-based deployment tool published by Insight Data Science, is used to deploy the three clusters (Kafka, Spark, and Cassandra) used in this project. All the necessary scripts can be found in the "deployment" directory. View the DEPLOYMENT_README.md  for detailed instructions on configuring and deploying the three clusters. Note that an AWS account and PEM key are necessary to deploy clusters via this method.

2. Once the three clusters are spun up, an XML copy of a Discogs data dump will need to be used. For the purposes of the demo, I have made a copy of the June data dump available on S3 (https://console.aws.amazon.com/s3/buckets/discogs-recommender/dumps/), but if you wish to use a more current (or previous) data dump, Discogs maintains a repository here: http://data.discogs.com/.

3. In the "kafka_cluster_scripts" directory, you will find all the necessary scripts to create the Kafka topics and start the producers. start_kafka_producers.sh will create the topics and start each producer, or the collection and activity producers may be started independently by running the commands included in the shell script. Each producer will run until canceled in the terminal.
