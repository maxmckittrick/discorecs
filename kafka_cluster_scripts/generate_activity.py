#generate pseudo-random activity for a given discogs user from cassandra and post to kafka topic (not working)

$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic user-collection-stream
