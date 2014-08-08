#!/bin/sh

boxCount=2
members=2
workers=2
duration=1m

provisioner --scale $boxCount

coordinator --memberWorkerCount $members \
	--clientWorkerCount $workers \
	--duration $duration \
	--workerVmOptions "-XX:+HeapDumpOnOutOfMemoryError" \
	--parallel \
	../../test.properties

provisioner --download

provisioner --terminate
