#!/bin/bash
#
# An example worker.sh configuration to generate flamegraphs using linux perf. So copy this
# file to your working directory and rename it to 'worker.sh' and make sure the test runs
# for at least 10 minutes.

# Automatic exit on script failure.
set -e

# Printing the command being executed (useful for debugging)
#set -x

# redirecting output/error to the right logfiles.
exec > worker.out
exec 2>worker.err

#JAVA_HOME=~/java/jdk1.8.0_121
#PATH=$JAVA_HOME/bin:$PATH

# If you want to impose having the right governor; enable the following
#old_governor=$(sudo cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
#echo old governor: $old_governor
#sudo cpupower frequency-set -g performance

JVM_ARGS="-XX:OnOutOfMemoryError=\"touch;-9;worker.oome\" \
          -Dlog4j.configuration=file:log4j.xml"

# Include the member/client-worker jvm options
JVM_ARGS="$JVM_OPTIONS $JVM_ARGS -XX:+PreserveFramePointer"

MAIN=com.hazelcast.simulator.worker.Worker

git clone --depth=1 https://github.com/jrudolph/perf-map-agent
cd perf-map-agent
cmake .
make
cd ..
cp perf-map-agent/out/attach-main.jar .
cp perf-map-agent/out/libperfmap.so .
git clone --depth=1 https://github.com/brendangregg/FlameGraph

java -classpath "$CLASSPATH" $JVM_ARGS $MAIN &

sleep 2m

worker_pid=$(cat worker.pid)
echo worker_pid $worker_pid

echo "Perf record"
# for overview of interesting perf settings
# http://www.brendangregg.com/perf.html
sudo perf record -F 99 -a -g -- sleep 180
echo "Perf record:DONE"

echo JAVA_HOME=$JAVA_HOME

echo "Run once"
java -cp attach-main.jar:$JAVA_HOME/lib/tools.jar net.virtualvoid.perf.AttachOnce $worker_pid
echo "Run once:DONE"

echo "Changing ownership of perf-*.map files"
sudo chown root /tmp/perf-*.map
echo "Changing ownership of perf-*.map files:DONE"

echo "Creating framegraph"
sudo perf script -i perf.data | FlameGraph/stackcollapse-perf.pl | FlameGraph/flamegraph.pl --color=java --hash > flamegraph.svg
echo "Creating framegraph:DONE"

#exit

wait

