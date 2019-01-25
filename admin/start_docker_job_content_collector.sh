#!/usr/bin/env bash

SHELL=/bin/bash

TIMESTAMP=`date +%Y%m%d%H%M%S`


DOCKER_BASE=/swissbib/harvesting/docker.images

cd $DOCKER_BASE


#docker run --rm  -v $DOCKER_BASE/configs/oai/$1.oai.yaml:/inrep -v $DOCKER_BASE/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep

echo "start container for $1 at : ${TIMESTAMP}\n" >> ${DOCKER_BASE}/container.log
docker run --rm  -v $DOCKER_BASE/logs/producer/json:/logpath -v $DOCKER_BASE/configs/oai/$1.oai.yaml:/inrep -v $DOCKER_BASE/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep

TIMESTAMP=`date +%Y%m%d%H%M%S`
echo "finished container for $1 at : ${TIMESTAMP}\n" >> ${DOCKER_BASE}/container.log