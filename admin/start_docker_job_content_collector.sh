#!/usr/bin/env bash

SHELL=/bin/bash

DOCKER_BASE=/swissbib/harvesting/docker.images

cd $DOCKER_BASE


docker run --rm  -v $DOCKER_BASE/configs/oai/$1.oai.yaml:/inrep -v $DOCKER_BASE/configs/oai/oai.share.yaml:/inshare kafka-event-hub run.py --type=OAIProducerKafka --sharedconfig /inshare /inrep


