#!/bin/sh

boxCount=$1
members=$2
clients=$3
duration=$4
output=output

gcArgs="-verbose:gc -Xloggc:verbosegc.log"
gcArgs="$gcArgs -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime"

memberHeapSZ=2G
partitions=271
monitorSec=30

memberJvmArgs="-Dhazelcast.partition.count=$partitions"
memberJvmArgs="$memberJvmArgs -Dhazelcast.health.monitoring.level=NOISY -Dhazelcast.health.monitoring.delay.seconds=$monitorSec"
memberJvmArgs="$memberJvmArgs -Xmx$memberHeapSZ -XX:+HeapDumpOnOutOfMemoryError"
memberJvmArgs="$memberJvmArgs $gcArgs"

clientHeapSZ=1G
clientJvmArgs="-Xmx$clientHeapSZ -XX:+HeapDumpOnOutOfMemoryError"
clientJvmArgs="$clientJvmArgs $gcArgs"

if ! provisioner --scale ${boxCount} ; then
  exit 1
fi

provisioner --clean
provisioner --restart

coordinator --memberWorkerCount ${members} \
	        --clientWorkerCount ${clients} \
	        --duration ${duration} \
	        --workerVmOptions "$memberJvmArgs" \
                --clientWorkerVmOptions "$clientJvmArgs" \
	        --parallel \
	        test.properties

provisioner --download ${output}

if [ -f nohup.out ]; then
  runId=$(grep -oh -m 1 "Starting testsuite: [0-9].*" nohup.out | cut -d ' ' -f3)
  mv nohup.out nohup-${runId}.out
fi

mv *.out ${output}/${runId}

mv failures-* ${output} 2>/dev/null
if [ -f ${output}/failures* ] ; then
    echo "FAIL! ${output}"
    exit 1
fi