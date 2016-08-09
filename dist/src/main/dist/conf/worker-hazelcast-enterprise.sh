#!/bin/bash
#
# Script to start up a Simulator Worker. To customize the behavior of the worker, including Java configuration,
# copy this file into the 'work dir' of simulator. See the end of this file for examples for different profilers.
#
# External variables have a @ symbol in front to distinguish them from regular bash variables.
#

# Automatic exit on script failure.
set -e

# Printing the command being executed (useful for debugging)
#set -x

# redirecting output/error to the right logfiles.
exec > worker.out
exec 2>worker.err

JVM_ARGS="-XX:OnOutOfMemoryError=\"touch;-9;worker.oome\" \
          -Dhazelcast.logging.type=log4j \
          -Dlog4j.configuration=file:@LOG4J_FILE \
          -DSIMULATOR_HOME=@SIMULATOR_HOME \
          -DpublicAddress=@PUBLIC_ADDRESS \
          -DagentIndex=@AGENT_INDEX \
          -DworkerType=@WORKER_TYPE \
          -DworkerId=@WORKER_ID \
          -DworkerIndex=@WORKER_INDEX \
          -DworkerPort=@WORKER_PORT \
          -DworkerPerformanceMonitorIntervalSeconds=@WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS \
          -DautoCreateHzInstance=@AUTO_CREATE_HZ_INSTANCE \
          -DhzConfigFile=@HZ_CONFIG_FILE"

# Include the member/client-worker jvm options
JVM_ARGS="@JVM_OPTIONS ${JVM_ARGS}"

CLASSPATH=@CLASSPATH

MAIN=
case "@WORKER_TYPE" in
    CLIENT )        MAIN=com.hazelcast.simulator.worker.ClientWorker;;
    MEMBER )        MAIN=com.hazelcast.simulator.worker.MemberWorker;;
    INTEGRATION_TEST )   MAIN=com.hazelcast.simulator.worker.IntegrationTestWorker;;
esac

# for more detail regarding profiling, check worker-hazelcast.sh

java -classpath ${CLASSPATH} ${JVM_ARGS} ${MAIN}

