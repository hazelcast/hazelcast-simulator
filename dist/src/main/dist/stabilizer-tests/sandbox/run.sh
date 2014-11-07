#!/bin/sh

boxCount=3
dedicatedMemberBox=2
members=2
clients=2

duration=1m
maxRuns=1

versions=(3.2.6)

jvmArgs="-Dhazelcast.partition.count=2711 -Dhazelcast.health.monitoring.level=NOISY -Dhazelcast.health.monitoring.delay.seconds=30"
jvmArgs="$jvmArgs -Xmx1000m -XX:+HeapDumpOnOutOfMemoryError"
jvmArgs="$jvmArgs -verbose:gc -XX:+PrintGCTimeStamps -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -Xloggc:verbosegc.log"

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
		        --workerClassPath "/home/danny/.m2/repository/org/hdrhistogram/HdrHistogram/1.2.1/HdrHistogram-1.2.1.jar" \
                sandBoxTest.properties


      provisioner --download results/$hzVersion
  done
done
echo "The End"

