#!/bin/sh

boxCount=3
dedicatedMemberBox=2
members=2
clients=2

partitions=271
jvmArgs="-Dhazelcast.initial.min.cluster.size=$members -Dhazelcast.partition.count=$partitions"
jvmArgs="$jvmArgs -Dhazelcast.health.monitoring.level=NOISY -Dhazelcast.health.monitoring.delay.seconds=30"
jvmArgs="$jvmArgs -Xmx1000m -XX:+HeapDumpOnOutOfMemoryError"
jvmArgs="$jvmArgs -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:verbosegc.log"

versions=(3.2.6 3.3.3)
duration=1m
maxRuns=1

for hzVersion in ${versions[@]}
do
  sed -i s/maven=.*/maven=$hzVersion/g stabilizer.properties

  for (( i=0; i<$maxRuns; i++ ))
  do
      provisioner --scale $boxCount
      ./initSysStats.sh
      provisioner --clean
      provisioner --restart

      coordinator --dedicatedMemberMachines $dedicatedMemberBox \
                --memberWorkerCount $members \
                --clientWorkerCount $clients \
                --duration $duration \
                --workerVmOptions "$jvmArgs" \
                --parallel \
		        --workerClassPath "~/.m2/repository/org/hdrhistogram/HdrHistogram/1.2.1/HdrHistogram-1.2.1.jar" \
                sandBoxTest.properties


      provisioner --download output/$hzVersion
  done
done
echo "The End"

