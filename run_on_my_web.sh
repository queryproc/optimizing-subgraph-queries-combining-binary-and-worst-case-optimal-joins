#!/bin/bash

sudo ./gradlew clean build installDist
. ./env.sh
cd scripts