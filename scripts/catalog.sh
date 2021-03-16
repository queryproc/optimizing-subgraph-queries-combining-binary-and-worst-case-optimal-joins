#!/bin/bash

cd ..
./gradlew clean build installDist
. ./env.sh
cd ./scripts
python3 serialize_catalog.py ./output/data