#!/bin/bash

clients=1
members=3

gc_log="-Xloggc:gc.log -XX:+PrintGC -XX:+PrintGCDetails \
        -XX:+PrintGCTimeStamps  -XX:+PrintGCDateStamps"

jfr="-XX:+UnlockCommercialFeatures \
     -XX:+FlightRecorder \
     -XX:StartFlightRecording=duration=3600s,filename=recording.jfr,settings=upload/allocation.jfc \
     -XX:+UnlockDiagnosticVMOptions \
     -XX:+DebugNonSafepoints "

diagnostics="-Dhazelcast.diagnostics.enabled=true \
            -Dhazelcast.diagnostics.metric.level=debug \
            -Dhazelcast.diagnostics.metrics.period.seconds=5 \
            -Dhazelcast.diagnostics.max.rolled.file.size.mb=1000"

provisioner --install

for i in `seq 1 15`;
do
	echo iteration $i

	agent-ssh "sudo killall -9 java || true"

	coordinator --duration 3m \
	            --driver hazelcast5 \
              --sessionId "hz5/@it" \
              --clients $clients \
              --clientType "javaclient" \
              --clientArgs "-Xms3g -Xmx3g $gc_log" \
              --members $members \
              --dedicatedMemberMachines $members \
              --memberArgs "-Xms3G -Xmx3G $gc_log -Dhazelcast.initial.min.cluster.size=$members" \
              test.properties
done
