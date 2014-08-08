#!/bin/sh

boxCount=6
members=6
workers=20
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
