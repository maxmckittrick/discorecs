# Cluster Deployment
To deploy clusters exactly as I've configred them for my demo, use the scripts in each of these three directories to spin up three seperate clusters for Kafka, Spark, and Cassandra. To tweak the amount/type of nodes in each of these clusters, edit the corresponding .yml file. Note that a Spark cluster requires a master node, while Cassandra and Kafka do not. All deployment for these clusters will be managed through Pegasus.

# Setup
1. Follow the steps in the Pegasus installation guide to clone the Pegasus repo and install Pegasus on your local machine: https://github.com/InsightDataScience/pegasus

2. To use Pegasus, an AWS account and PEM key are required. For the purposes of this deployment process, your PEM key should be accessible on your machine as `~/.ssh/key-pair.pem`. Verify your installation of Pegasus with `peg config`. If successful, you will see your access key, secret key, region, and SSH user printed to the console.

3. Additionally, the scripts in this directory expect two .txt files in your `~/.ssh/` directory: `sg.txt`, which is a text file with one line listing your security group (e.g.; sg-XXX), and similarly, `subnet.txt`, a text file with one line listing your subnet (e.g.; subnet-XXX). If you're not sure what to put in these two files, you can view your security groups with `peg aws security-groups`, and your subnets with `peg aws subnets`. 

3. You are now ready to deploy the clusters. The scripts I've included will automatically install all the necessary technologies on the clusters and start the necessary services. For example, to deploy the Cassandra cluster:
`$ ~/discorecs/deployment/cassandra/cassandra.sh`
