#!/usr/bin/env bash

KAFKA_BASE=/usr/local/swissbib/kafka
ZK_HOST=sb-uka1.swissbib.unibas.ch:4181,sb-uka2.swissbib.unibas.ch:4181,sb-uwf3.swissbib.unibas.ch:4181/kafka

#for topic in IDSABN IDSALEX ALEXREPO IDSBGR BORIS ECOD EDOC EPERIODICA ETHRESEARCH HEMU IDSBB IDSLU IDSSG KBTG LIBIB NATIONALBIBLIOTHEK POSTERS IDSSBT SERVAL SGBN VAUD ZORA
for topic in IDSABN IDSALEX ALEXREPO IDSBGR BORIS ECOD EDOC EPERIODICA ETHRESEARCH HEMU IDSBB


do
    echo "delete topic $topic"

    ssh swissbib@sb-uka1.swissbib.unibas.ch "source /home/swissbib/.bash_profile; $KAFKA_BASE/bin/kafka-topics.sh --zookeeper $ZK_HOST --delete --topic $topic"

done
