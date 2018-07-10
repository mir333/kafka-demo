#!/usr/bin/env bash


./bin/confluent  start

./bin/kafka-topics --zookeeper localhost:2181 --create --topic basic-file-data --partitions 1 --replication-factor 1

./bin/kafka-console-consumer  --bootstrap-server localhost:9092 --topic  basic-file-data --from-beginning
./bin/kafka-console-consumer  --bootstrap-server localhost:9092 --topic processed-file-data --from-beginning

./bin/confluent  stop connect
./bin/connect-standalone /data/git/mir333/kafka-demo/etc/connect-standalone.properties /data/git/mir333/kafka-demo/etc/elastic.properties
