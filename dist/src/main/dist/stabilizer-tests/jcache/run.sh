#!/bin/sh

boxCount=2
members=2
clients=2
duration=2m

for i in {1..1}
do

    provisioner --scale $boxCount
    provisioner --clean
    provisioner --restart

    coordinator --memberWorkerCount $members \
        --clientWorkerCount $clients \
        --duration $duration \
        --workerVmOptions "-XX:+HeapDumpOnOutOfMemoryError" \
        --parallel \
        jcacheTest.properties

    provisioner --download

done

echo "The End"
