#!/usr/bin/env bash


EVENT_HUB_BASE=/home/swissbib/environment/code/swissbib.repositories/kafka-event-hub
TARGET=/swissbib/harvesting/docker.images

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

echo "create logdir and logfiles if not available"
ssh harvester@sb-ucoai2.swissbib.unibas.ch "[ ! -d ${TARGET/logs} ]  &&  mkdir -p ${TARGET}/logs/producer/json && \
        touch ${TARGET}/logs/producer/json/times.log && touch ${TARGET}/logs/producer/json/error.log"


echo "create other required dirs"
ssh harvester@sb-ucoai2.swissbib.unibas.ch "[ ! -d ${TARGET/rero-src} ]  &&  mkdir -p ${TARGET}/rero-src"
ssh harvester@sb-ucoai2.swissbib.unibas.ch "[ ! -d ${TARGET/rero-working} ]  &&  mkdir -p ${TARGET}/rero-working"

ssh harvester@sb-ucoai2.swissbib.unibas.ch "[ ! -d ${TARGET/incomingnebis} ]  &&  mkdir -p ${TARGET}/incomingnebis"
ssh harvester@sb-ucoai2.swissbib.unibas.ch "[ ! -d ${TARGET/nebis-src} ]  &&  mkdir -p ${TARGET}/nebis-src"
ssh harvester@sb-ucoai2.swissbib.unibas.ch "[ ! -d ${TARGET/nebis-working} ]  &&  mkdir -p ${TARGET}/nebis-working"



echo "cp config and admin scripts on target host"
scp -r admin configs harvester@sb-ucoai2.swissbib.unibas.ch:$TARGET

rm kafka-event-hub.tar