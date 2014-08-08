#!/bin/sh

boxCount=10
members=10
workers=50
duration=12h

provisioner --scale $boxCount

coordinator --memberWorkerCount $members \
	--clientWorkerCount $workers \
	--duration $duration \
	--workerVmOptions "-XX:+HeapDumpOnOutOfMemoryError" \
	--parallel \
	../../test.properties

provisioner --download

provisioner --terminate
