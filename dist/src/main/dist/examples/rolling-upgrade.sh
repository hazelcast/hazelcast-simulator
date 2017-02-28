#!/bin/bash
#
# Verifies that rolling upgrade for members works.
# This is supported since Hazelcast EE 3.8 (for minor versions only).

# We begin with a cluster with old version.
# Then we shutdown each member with an old version and restart it with a new version.
#

set -e

# the number of members we are going to start with
members=10
clients=2
old_version=maven=3.8
new_version=maven=3.9-SNAPSHOT

# start the Coordinator in remote mode
coordinator &

coordinator-remote install ${old_version}
coordinator-remote install ${new_version}

echo "Starting members..."
coordinator-remote worker-start --count ${members} --versionSpec ${old_version}

echo "Starting client drivers..."
coordinator-remote worker-start --count ${clients} --workerType javaclient --versionSpec ${old_version}

echo "Starting the test..."
test_id=$(coordinator-remote test-start --duration 0s rolling.properties)
echo "test_id: $test_id"

echo "Waiting for the test to run..."
sleep 30s

for i in {1..$members}
do
   echo iteration ${i}

   echo "Killing member with old version ($old_version)"
   coordinator-remote worker-kill --versionSpec ${old_version}

   echo "Starting member with new version ($new_version)"
   coordinator-remote worker-start --versionSpec ${new_version}

   sleep 10s

   status=$(coordinator-remote test-status ${test_id})
   echo "test_status: $status"
   if [ ${status} == "failure" ]; then
       echo "Error detected!!!"
       break
   fi
done

coordinator-remote test-stop ${test_id}
coordinator-remote stop
