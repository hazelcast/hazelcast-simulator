#!/bin/sh

boxCount=2
members=2
workers=2
duration=2m

provisioner --scale $boxCount
provisioner --clean
provisioner --restart

coordinator --memberWorkerCount $members \
	--clientWorkerCount $workers \
	--duration $duration \
	--workerVmOptions "-XX:+HeapDumpOnOutOfMemoryError" \
	--parallel \
	sandBoxTest.properties

provisioner --download

echo "The End"