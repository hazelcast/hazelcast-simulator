#!/bin/sh

boxCount=$1
members=$2
workers=$3
duration=$4

provisioner --scale $boxCount

coordinator --memberWorkerCount $members \
	--clientWorkerCount $workers \
	--duration $duration \
	--workerVmOptions "-XX:+HeapDumpOnOutOfMemoryError" \
	--parallel \
	../../test.properties

provisioner --download

provisioner --terminate

