#!/bin/bash
#
# Script to start up a Simulator Worker.
#
# To customize the behavior of the Worker, including Java configuration, copy this file into the 'work dir' of Simulator.
#

# automatic exit on script failure
set -e
# printing the command being executed (useful for debugging)
#set -x

# redirecting output/error to the right log files
exec > worker.out
exec 2> worker.err

echo ${LOG4j_CONFIG} > log4j.xml
echo ${HAZELCAST_CONFIG} > hazelcast.xml

JVM_ARGS="-XX:OnOutOfMemoryError=\"touch;-9;worker.oome\" \
          -Dhazelcast.logging.type=log4j \
          -Dlog4j.configuration=file:log4j.xml \
          -DSIMULATOR_HOME=$SIMULATOR_HOME \
          -DpublicAddress=$PUBLIC_ADDRESS \
          -DagentIndex=$AGENT_INDEX \
          -DworkerType=$WORKER_TYPE \
          -DworkerId=$WORKER_ID \
          -DworkerIndex=$WORKER_INDEX \
          -DworkerPort=$WORKER_PORT \
          -DworkerPerformanceMonitorIntervalSeconds=$WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS \
          -DautoCreateHzInstance=$AUTOCREATE_HAZELCAST_INSTANCE \
          -DhzConfigFile=hazelcast.xml"

# Include the member/client-worker jvm options
JVM_ARGS="$JVM_OPTIONS $JVM_ARGS"

MAIN=com.hazelcast.simulator.worker.MemberWorker

java -classpath "$CLASSPATH" ${JVM_ARGS} ${MAIN}
