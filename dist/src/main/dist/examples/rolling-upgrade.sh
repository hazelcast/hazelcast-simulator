#!/bin/bash

#
# This test verifies that rolling upgrade works. We begin with a cluster with an old version, and then
# we shutdown member with old version, and restart it with a new version.

set -e

# The number of members we are going to start with.
members=10
clients=2
old_version=maven=3.7
new_version=maven=3.8

# start the coordinator in remote mode
coordinator --remote --monitorPerformance &

coordinator-remote install ${old_version}
coordinator-remote install ${new_version}

echo starting members
coordinator-remote worker-start --count ${members} --versionSpec ${old_version}

echo starting client drivers
coordinator-remote worker-start --count ${clients} --workerType javaclient --versionSpec ${old_version}

echo starting the test
test_id=$(coordinator-remote test-start --duration 0s atomiclong.properties)
echo test_id $test_id

echo waiting for test to run
sleep 30s

for i in {1..$members}
do
   echo iteration $i

   echo killing member
   coordinator-remote worker-kill --versionSpec ${old_version}

   echo starting new member
   coordinator-remote worker-start --versionSpec ${new_version}

   sleep 10s

   status=$(coordinator-remote test-status $test_id)
   echo test_status $status
   if [ $status == "failure" ]; then
       echo Error detected!!!!!!!
       break
   fi
done

coordinator-remote test-stop $test_id
coordinator-remote stop
