#!/bin/bash

set -e

coordinator     --workerVmOptions "-ea -server -Xms2G -Xmx2G -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:gc.log -XX:+HeapDumpOnOutOfMemoryError" \
                --clientHzFile      client-hazelcast.xml \
                --hzFile            conf/hazelcast.xml \
                --clientWorkerCount 2 \
                --memberWorkerCount 2 \
                --workerClassPath   '../target/*.jar' \
                --duration          5m \
                --monitorPerformance \
                test.properties

provisioner --download

