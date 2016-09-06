#!/bin/bash

#
# This test starts a set of members, and a set of clients, and while the clients are executing some test
# the members are by one one shutdown and restart.
#

set -e

# The command we are going to use. Javascript or bashscript is possible. See 'coordinator-remote kill --help' for more info.
kill_command=System.exit

# The number of times we are going to kill and start a member.
iterations=5

echo starting 2 members
coordinator-remote worker-start --count 2

echo starting 2 client drivers
coordinator-remote worker-start --count 2 --workerType javaclient

echo starting the test
test_id=$(coordinator-remote test-start --duration 0s atomiclong.properties)
echo test_id $test_id

echo waiting for test to run
sleep 30s

for i in {1..$iterations}
do
   echo iteration $i

   echo killing member
   coordinator-remote worker-kill --command ${kill_command}

   echo starting new member
   coordinator-remote worker-start

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
