#!/bin/bash

#
# Script to start up a Simulator Worker.
#
# To customize the behavior of the Worker, including Java configuration, copy this file into the 'work dir' of Simulator.
# See the end of this file for examples for different profilers.
#

# automatic exit on script failure
set -e
# printing the command being executed (useful for debugging)
#set -x

# redirecting output/error to the right log files
exec > worker.out
exec 2> worker.err


# If you want to be sure that you have the right governor installed; uncomment
# the following 3 lines. They will force the right governor to be used.
#old_governor=$(sudo cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
#echo old_governor: $old_governor
#sudo cpupower frequency-set -g performance

# If you have a specific java version you want to use, uncomment the following lines
# and configure the path to the version to use.
#JAVA_HOME=~/java/jdk1.8.0_121
#PATH=$JAVA_HOME/bin:$PATH

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

if [ "$WORKER_TYPE" = "javaclient" ] ; then
    MAIN=com.hazelcast.simulator.worker.ClientWorker
else
    MAIN=com.hazelcast.simulator.worker.MemberWorker
fi

java -classpath "$CLASSPATH" ${JVM_ARGS} ${MAIN}

#########################################################################
# Yourkit
#########################################################################
#
# When YourKit is enabled, a snapshot is created an put in the worker home directory.
# So when the artifacts are downloaded, the snapshots are included and can be loaded with your YourKit GUI.
#
# To upload the libyjpagent, create a 'upload' directory in the working directory and place the libypagent.so there.
# Then it will be automatically uploaded to all workers.
#
# For more information about the YourKit setting, see:
#   http://www.yourkit.com/docs/java/help/agent.jsp
#   http://www.yourkit.com/docs/java/help/startup_options.jsp
#
# java -agentpath:$(pwd)/libyjpagent.so=dir=$(pwd),sampling -classpath $CLASSPATH $JVM_ARGS $MAIN
#

#########################################################################
# HProf
#########################################################################
#
# By default a 'java.hprof.txt' is created in the worker directory.
# The file will be downloaded by the Coordinator after the test has run.
#
# For configuration options see:
#   http://docs.oracle.com/javase/7/docs/technotes/samples/hprof.html
#
# java -agentlib:hprof=cpu=samples,depth=10 -classpath $CLASSPATH $JVM_ARGS $MAIN
#

#########################################################################
# Linux Perf
#########################################################################
#
# https://perf.wiki.kernel.org/index.php/Tutorial#Sampling_with_perf_record
#
# The settings is the full commandline for 'perf record' excluding the actual arguments for the java program to start.
# These will be provided by the Simulator Agent.
#
# Once the test run completes, all the artifacts (including the perf.data created by perf) will be downloaded by the Coordinator.

# Another option is to log into the Agent machine and do a 'perf report' locally.
#
# TODO:
# More work needed on documentation to get perf running correctly.
#
# If you get the following message:
#           Kernel address maps (/proc/{kallsyms,modules}) were restricted.
#           Check /proc/sys/kernel/kptr_restrict before running 'perf record'.
# Apply the following under root:
#           echo 0 > /proc/sys/kernel/kptr_restrict
# To make it permanent, add it to /etc/rc.local
#
# If you get the following message while doing call graph analysis (-g)
#            No permission to collect stats.
#            Consider tweaking /proc/sys/kernel/perf_event_paranoid.
# Apply the following under root:
#           echo -1 > /proc/sys/kernel/perf_event_paranoid
# To make it permanent, add it to /etc/rc.local
#
# perf record -o perf.data --quiet java -classpath $CLASSPATH $JVM_ARGS $MAIN
#

#########################################################################
# VTune
#########################################################################
#
# It requires Intel VTune to be installed on the system.
#
# The settings is the full commandline for the amplxe-cl excluding the actual arguments for the java program to start.
# These will be provided by the Simulator Agent.
#
# Once the test run completes, all the artifacts will be downloaded by the Coordinator.
#
# To see within the JVM, make sure that you locally have the same Java version (under the same path) as the simulator.
# Else VTune will not be able to see within the JVM.
#
# Reference to amplxe-cl commandline options:
# https://software.intel.com/sites/products/documentation/doclib/iss/2013/amplifier/lin/ug_docs/GUID-09766DB6-3FA8-445B-8E70-5BC9A1BE7C55.htm#GUID-09766DB6-3FA8-445B-8E70-5BC9A1BE7C55
#
# /opt/intel/vtune_amplifier_xe/bin64/amplxe-cl -collect hotspots java -classpath $CLASSPATH $JVM_ARGS $MAIN
#

#########################################################################
# NUMA Control
#########################################################################
#
# NUMA Control. It allows to start member with a specific numactl settings.
# numactl binary has to be available on $PATH
#
# Example: NUMA_CONTROL=numactl -m 0 -N 0
# It will bind members to node 0.
# numactl -m 0 -N 0 java -classpath $CLASSPATH $JVM_ARGS $MAIN
#

#########################################################################
# DStat
#########################################################################
#
# dstat --epoch -m --all -l --noheaders --nocolor --output dstat.csv 5 > /dev/null &
# java -classpath $CLASSPATH $JVM_ARGS $MAIN
# kill $(jobs -p)
#

#########################################################################
# OpenOnload
#########################################################################
#
# The network stack for Solarflare network adapters (new lab).
#
# onload --profile=latency java -classpath $CLASSPATH $JVM_ARGS $MAIN
#
