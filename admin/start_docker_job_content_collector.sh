#!/usr/bin/env bash

SHELL=/bin/bash

TIMESTAMP=`date +%Y%m%d%H%M%S`


DOCKER_BASE=/swissbib/harvesting/docker.images

cd $DOCKER_BASE


#docker run --rm  -v $DOCKER_BASE/configs/oai/$1.oai.yaml:/inrep -v $DOCKER_BASE/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep

echo "start container for $1 at : ${TIMESTAMP}\n" >> ${DOCKER_BASE}/container.log

#docker container run --rm      -v $(pwd)/logpath:/logpath  -v $(pwd):basedir -v $(pwd)/configs/oai:/inrep -v $(pwd)/configs/logs:/logconfigpath -v $(pwd)/configs/share:/inshare kafka-event-hub app/run.py --type=OAIProducerKafka --sharedconfig /inshare/cc.share.yaml /inrep/$1.oai.yaml
#docker run --rm  -v $DOCKER_BASE/logs/producer/json:/logpath -v $DOCKER_BASE/configs/oai/$1.oai.yaml:/inrep -v $DOCKER_BASE/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep

#neu mit NEBIS / RERO (konkret Nebis)

echo "docker run --rm  -v $DOCKER_BASE:/basedir   -v $DOCKER_BASE/logs/producer/json:/logpath -v $DOCKER_BASE/configs/filepush/$1.yaml:/inrep -v $DOCKER_BASE/configs/logs:/logconfigpath -v $DOCKER_BASE/configs/share/cc.share.yaml:/inshare kafka-event-hub run.py --type=FilePushNebisKafka --sharedconfig /inshare /inrep"

docker run --rm  -v $DOCKER_BASE:/basedir   -v $DOCKER_BASE/logs/producer/json:/logpath -v $DOCKER_BASE/configs/filepush/$1.yaml:/inrep -v $DOCKER_BASE/configs/logs:/logconfigpath -v $DOCKER_BASE/configs/share/cc.share.yaml:/inshare kafka-event-hub run.py --type=FilePushNebisKafka --sharedconfig /inshare /inrep


TIMESTAMP=`date +%Y%m%d%H%M%S`
echo "finished container for $1 at : ${TIMESTAMP}\n" >> ${DOCKER_BASE}/container.log
