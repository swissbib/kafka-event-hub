#!/bin/bash
docker kill zookeeper
docker kill kafka

docker rm zookeeper
docker rm kafka