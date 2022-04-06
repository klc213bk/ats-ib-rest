#!/bin/bash

# list topics
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# create topics
/opt/kafka/bin/kafka-topics.sh --create --topic TWS.ACCOUNT --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-topics.sh --create --topic TWS.ACCOUNT_DOWNLOAD_END --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-topics.sh --create --topic TWS.ACCOUNT_VALUE --partitions 1 -- eplication-factor 2 --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-topics.sh --create --topic TWS.PORTFOLIO --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-topics.sh --create --topic TWS.ACCOUNT_TIME --partitions 1 --replication-factor 2 --bootstrap-server localhost:9092

