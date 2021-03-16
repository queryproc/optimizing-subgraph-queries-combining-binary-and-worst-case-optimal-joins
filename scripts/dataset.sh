#!/bin/bash

cd ..
./gradlew clean build installDist
. ./env.sh
cd ./scripts
python3 serialize_dataset.py ./output/data.csv ./output/data