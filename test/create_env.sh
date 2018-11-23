#!/bin/bash

mkdir -p kafka/{data,logs} && cd kafka/
docker run -d --name zookeeper --network host --publish 2181:2181 zookeeper:3.4
docker run -d \
    --network host \
    --name kafka \
    --volume `pwd`/data:/data --volume `pwd`/logs:/logs \
    --publish 9092:9092 --publish 7203:7203 \
    --env KAFKA_ADVERTISED_HOST_NAME=127.0.0.1 --env ZOOKEEPER_IP=127.0.0.1 \
    ches/kafka