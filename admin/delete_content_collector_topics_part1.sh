#!/usr/bin/env bash

KAFKA_BASE=/usr/local/swissbib/kafka
KAFKA_BASE1=/usr/local/swissbib/kafka1
ZK_HOST=sb-uka1.swissbib.unibas.ch:4181,sb-uka2.swissbib.unibas.ch:4181,sb-uwf3.swissbib.unibas.ch:4181/kafka
ZK_HOST_KAFKA=localhost:4181

#for topic in IDSABN IDSALEX ALEXREPO IDSBGR BORIS ECOD EDOC EPERIODICA ETHRESEARCH HEMU IDSBB IDSLU IDSSG KBTG LIBIB NATIONALBIBLIOTHEK POSTERS IDSSBT SERVAL SGBN VAUD ZORA
for topic in IDSABN ALEX ALEXREPO IDSBGR BORIS ECOD EDOC EPERIODICA ETHRESEARCH HEMU IDSBB RERO


do
    echo "delete topic $topic"

    ssh swissbib@sb-uka1.swissbib.unibas.ch "source /home/swissbib/.bash_profile; $KAFKA_BASE/bin/kafka-topics.sh --zookeeper $ZK_HOST --delete --topic $topic"
    #$KAFKA_BASE1/bin/kafka-topics.sh --zookeeper $ZK_HOST_KAFKA --delete --topic $topic


done
