#!/bin/bash

export PYTHONPATH=/home/swissbib/environment/code/swissbib.repositories/dataIngestion:$PYTHONPATH

kafkaPython=/home/swissbib/env.local/tools/pydataingestion/bin/python3
$kafkaPython ingest.py -c config/files/oai/bgr.oai.yaml