#!/usr/bin/env bash

TARGET_HOST=sb-uingest2.swissbib.unibas.ch
EVENT_HUB_BASE=/home/swissbib/environment/code/swissbib.repositories/kafka-event-hub
TARGET=/swissbib/harvesting/docker.cc

cd $EVENT_HUB_BASE ||  exit

echo "build new latest image kafka-event-hub"
#docker image build --no-cache -t kafka-event-hub -f docker.small/Dockerfile .
docker image build -t kafka-event-hub -f docker.small/Dockerfile .

echo "save latest image kafka-event-hub as tar file"
docker save kafka-event-hub --output kafka-event-hub.tar

ssh harvester@${TARGET_HOST} "[ ! -d ${TARGET} ]  &&  mkdir -p ${TARGET}"

echo "cp tar file to target host"
scp kafka-event-hub.tar harvester@${TARGET_HOST}:$TARGET

#scp -r configs/oai harvester@${TARGET_HOST}:$TARGET/configs
#scp -r configs/filepush harvester@${TARGET_HOST}:$TARGET/configs
#scp -r configs/share/cc.share.yaml harvester@${TARGET_HOST}:$TARGET/configs/share/


echo "rm already existing image om target host"
echo "load just created image on target host"
ssh harvester@${TARGET_HOST} "cd $TARGET; docker image rm kafka-event-hub; docker load --input kafka-event-hub.tar"

echo "create logdir and logfiles if not available"
ssh harvester@${TARGET_HOST} "[ ! -d ${TARGET/logs} ]  &&  mkdir -p ${TARGET}/logs/producer/json && \
        touch ${TARGET}/logs/producer/json/times.log && touch ${TARGET}/logs/producer/json/error.log"


echo "create other required dirs"
ssh harvester@${TARGET_HOST} "[ ! -d ${TARGET}/rero-src ] &&   echo \"create ${TARGET}/rero-src\" && mkdir -p ${TARGET}/rero-src"
ssh harvester@${TARGET_HOST} "[ ! -d ${TARGET}/rero-working ] && echo \"create ${TARGET}/rero-working\" &&  mkdir -p ${TARGET}/rero-working"

ssh harvester@${TARGET_HOST} "[ ! -d ${TARGET}/incomingnebis ] &&  echo \"create ${TARGET}/incomingnebis\" &&  mkdir -p ${TARGET}/incomingnebis"
ssh harvester@${TARGET_HOST} "[ ! -d ${TARGET}/nebis-src ] &&   echo \"create ${TARGET}/nebis-src\";  mkdir -p ${TARGET}/nebis-src"
ssh harvester@${TARGET_HOST} "[ ! -d ${TARGET}/nebis-working ] &&  echo \"create ${TARGET}/nebis-working\";  mkdir -p ${TARGET}/nebis-working"




#echo "cp config and admin scripts on target host"
scp -r admin configs harvester@${TARGET_HOST}.swissbib.unibas.ch:$TARGET

rm kafka-event-hub.tar

ssh harvester@${TARGET_HOST} "rm ${TARGET}/kafka-event-hub.tar"