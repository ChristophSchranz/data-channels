#!/bin/bash
# common commands:
sudo docker build -t kafkastack .
sudo docker rm -f kafkastack || true
sudo docker run --name kafkastack --net=host kafkastack

sudo docker build -t kafkastack . && sudo docker rm -f kafkastack || true && sudo docker run --name kafkastack --net=host kafkastack


# change 127.0.0.1 by your Docker ADV_HOST
# Get the command line from:
sudo docker run --rm -p 2181:2181 -p 3030:3030 -p 8081-8083:8081-8083 \
       -p 9581-9585:9581-9585 -p 9092:9092 -e ADV_HOST=iot86 \
       landoop/fast-data-dev:cp3.3.0

sudo docker run --rm --net=host -e ADV_HOST=iot86 \
       landoop/fast-data-dev:cp3.3.0


sudo docker run --net=host -it confluentinc/cp-schema-registry:3.3.0 bash

# Or download the Confluent Binaries at https://www.confluent.io/download/
# And add them to your PATH

# List topics
kafka-topics --list --zookeeper localhost:2181


# Create topics: TODO change replication factor to 3 in swarm
kafka-topics --create --zookeeper localhost:2181 --topic rest-binary --replication-factor 1 --partitions 1
kafka-topics --create --zookeeper localhost:2181 --topic rest-json --replication-factor 1 --partitions 1
kafka-topics --create --zookeeper localhost:2181 --topic rest-avro --replication-factor 1 --partitions 1

kafka-topics --create --zookeeper localhost:2181 --topic eu.company1.plant1.machine1.sensor1 --replication-factor 1 --partitions 1

kafka-topics --create --zookeeper localhost:2181 --topic eu.company1.plant1.machine1.sensor2 --replication-factor 1 --partitions 1 --cleanup.policy compact --flush.ms 60000 --retention.ms 604800000
kafka-topics --create --zookeeper localhost:2181 --topic eu.srfg.ultimaker.temp124 --replication-factor 1 --partitions 1 --config cleanup.policy=compact --config flush.ms=60000 --config retention.ms=604800000


# Consume data
kafka-avro-console-consumer --zookeeper localhost:2181 --topic eu.srfg.ultimaker.temp124

# Consume the records from the beginning of the topic:
kafka-avro-console-consumer --topic eu.srfg.ultimaker.temp124 \
    --bootstrap-server 127.0.0.1:9092 \
    --property schema.registry.url=http://127.0.0.1:8081 \
    --from-beginning


# Produce some errors with an incompatible schema (we changed to int) - should produce a 409
kafka-avro-console-producer \
    --broker-list localhost:9092 --topic eu.srfg.ultimaker.temp124 \
    --property schema.registry.url=http://127.0.0.1:8081 \
    --property value.schema='{"type":"int"}'


# Some schema evolution (we add a field f2 as an int with a default)
kafka-avro-console-producer \
    --broker-list localhost:9092 --topic eu.srfg.ultimaker.temp124 \
    --property schema.registry.url=http://127.0.0.1:8081 \
    --property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"},{"name": "f2", "type": "int", "default": 0}]}'

# {"f1": "evolution", "f2": 1 }

# Consume the records again from the beginning of the topic:
kafka-avro-console-consumer --topic eu.srfg.ultimaker.temp124 \
    --bootstrap-server localhost:9092 \
    --from-beginning \
    --property schema.registry.url=http://127.0.0.1:8081
