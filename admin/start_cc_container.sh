#!/usr/bin/env bash

SHELL=/bin/bash

TIMESTAMP=`date +%Y%m%d%H%M%S`


DOCKER_BASE=/swissbib/harvesting/docker.cc
WEBDAV=/swissbib/harvesting/rero.webdav
cd $DOCKER_BASE

echo "start container for $1 at : ${TIMESTAMP}\n" >> ${DOCKER_BASE}/container.log


if [ $1 == 'rero' ]
then
    echo "start rero webdav process"
    echo "used docker command"
    echo "docker run --rm -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=WebDavReroKafka --sharedconfig /basedir/configs/share/cc.share.yaml /basedir/configs/webdav/$1.yaml"
    docker run --rm -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=WebDavReroKafka --sharedconfig /basedir/configs/share/cc.share.yaml /basedir/configs/webdav/$1.yaml
elif [ $1 == 'nebis' ]
then

    echo "start nebis filepush process"
    echo "used docker command"
    echo "docker run --rm  -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=FilePushNebisKafka --sharedconfig /basedir/configs/share/cc.share.yaml /basedir/configs/filepush/$1.yaml"
    docker run --rm  -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=FilePushNebisKafka --sharedconfig /basedir/configs/share/cc.share.yaml /basedir/configs/filepush/$1.yaml
elif [ $1 == 'zem' ]
then

    echo "start zem edu process"
    echo "used docker command"
    echo "docker run --rm  -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=EduZemKafka --sharedconfig /basedir/configs/share/edu.share.yaml /basedir/configs/eduplatform/$1.yaml"
    docker run --rm  -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=EduZemKafka --sharedconfig /basedir/configs/share/edu.share.yaml /basedir/configs/eduplatform/$1.yaml


else

    echo "start oai process"
    echo "used docker command"
    echo "docker run --rm  -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /basedir/configs/share/cc.share.yaml /basedir/configs/oai/$1.yaml"
    docker run --rm  -v $WEBDAV:/webdav -v $DOCKER_BASE:/basedir   kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /basedir/configs/share/cc.share.yaml /basedir/configs/oai/$1.yaml


fi

TIMESTAMP=`date +%Y%m%d%H%M%S`
echo "finished container for $1 at : ${TIMESTAMP}\n" >> ${DOCKER_BASE}/container.log
