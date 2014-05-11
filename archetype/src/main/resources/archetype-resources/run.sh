#!/bin/bash

provisioner --scale 2

coordinator     --workerVmOptions "-ea -server -Xms2G -Xmx2G -XX:+PrintGC -XX:+HeapDumpOnOutOfMemoryError" \
                --clientHzFile      ../conf/client-hazelcast.xml \
                --hzFile            ../conf/hazelcast.xml \
                --clientWorkerCount 1 \
                --memberWorkerCount 1 \
                --workerClassPath   '../target/*.jar' \
                --duration          5m \
                ../conf/test.properties

provisioner --download

provisioner --terminate

