#!/usr/bin/env bash


EVENT_HUB_BASE=/home/swissbib/environment/code/swissbib.repositories/kafka-event-hub
TARGET=/swissbib/harvesting/docker.cc

cd $EVENT_HUB_BASE

echo "build new latest image kafka-event-hub"
docker image build -t kafka-event-hub -f docker.small/Dockerfile .

echo "save latest image kafka-event-hub as tar file"
docker save kafka-event-hub --output kafka-event-hub.tar

echo "cp tar file to target host"
scp kafka-event-hub.tar harvester@sb-ucoai2.swissbib.unibas.ch:$TARGET

echo "rm already existing image om target host"
echo "load just created image on target host"
ssh harvester@sb-ucoai2.swissbib.unibas.ch "cd $TARGET; docker image rm kafka-event-hub; docker load --input kafka-event-hub.tar"



echo "cp config and admin scripts on target host"
scp -r admin configs harvester@sb-ucoai2.swissbib.unibas.ch:$TARGET

rm kafka-event-hub.tar

ssh harvester@sb-ucoai2.swissbib.unibas.ch "rm ${TARGET}/kafka-event-hub.tar"