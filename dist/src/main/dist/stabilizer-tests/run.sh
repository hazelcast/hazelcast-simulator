#!/bin/sh

boxCount=$1
members=$2
workers=$3
duration=$4
output=$5

provisioner --scale ${boxCount}

coordinator --memberWorkerCount ${members} \
	        --clientWorkerCount ${workers} \
	        --duration ${duration} \
	        --workerVmOptions "-XX:+HeapDumpOnOutOfMemoryError" \
	        --parallel \
	        test.properties

provisioner --download ${output}

provisioner --terminate

mv failures-* ${output} 2>/dev/null

if [ -f ${output}/failures* ] ; then
    echo "FAIL! ${output}"
    exit 1
fi