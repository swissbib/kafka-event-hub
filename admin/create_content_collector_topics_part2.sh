#!/usr/bin/env bash

KAFKA_BASE=/usr/local/swissbib/kafka
ZK_HOST=sb-uka1.swissbib.unibas.ch:4181,sb-uka2.swissbib.unibas.ch:4181,sb-uwf3.swissbib.unibas.ch:4181/kafka

for topic in IDSLU IDSSG KBTG LIBIB NATIONALBIBLIOTHEK POSTERS IDSSBT SERVAL SGBN VAUD ZORA

do
    echo "create topic $topic"

    ssh swissbib@sb-uka1.swissbib.unibas.ch "source /home/swissbib/.bash_profile; \
    $KAFKA_BASE/bin/kafka-topics.sh --zookeeper $ZK_HOST --create --topic $topic --partitions 3 --replication-factor 1"

done
