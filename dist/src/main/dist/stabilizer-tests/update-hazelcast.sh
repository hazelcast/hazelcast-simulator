#!/bin/sh

hear=$(pwd)
cd ~/hazelcast

if ! git pull > /dev/null ; then
  exit 1
fi

if ! mvn clean install -DskipTests > build.txt ; then
  exit 1
fi

grep "Building Hazelcast Root.*" build.txt | awk 'NF>1{print $NF}'

cd $hear
