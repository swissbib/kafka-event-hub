#!/usr/bin/env bash

KAFKA_BASE=/usr/local/swissbib/kafka
KAFKA_BASE1=/usr/local/swissbib/kafka1
ZK_HOST=sb-uka1.swissbib.unibas.ch:4181,sb-uka2.swissbib.unibas.ch:4181,sb-uwf3.swissbib.unibas.ch:4181/kafka
ZK_HOST_KAFKA=localhost:4181/kafka

for topic in IDSABN ALEX ALEXREPO IDSBGR BORIS ECOD EDOC EPERIODICA ETHRESEARCH HEMU IDSBB

do
    echo "create topic $topic"

    ssh swissbib@sb-uka1.swissbib.unibas.ch "source /home/swissbib/.bash_profile; \
    $KAFKA_BASE/bin/kafka-topics.sh --zookeeper $ZK_HOST --create --topic $topic --partitions 3 --replication-factor 1"

    #$KAFKA_BASE1/bin/kafka-topics.sh --zookeeper $ZK_HOST_KAFKA  --create --topic $topic --partitions 3 --replication-factor 1


done
