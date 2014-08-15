#!/bin/sh

provisioner --scale 2
provisioner --clean
provisioner --restart

coordinator --memberWorkerCount 2 \
	--clientWorkerCount 2 \
	--duration 2m \
	--workerVmOptions "-XX:+HeapDumpOnOutOfMemoryError" \
	--parallel \
	sandBoxTest.properties

provisioner --download

echo "The End"