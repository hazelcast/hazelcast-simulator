#!/bin/bash

#
# An example test to run 2 different Hazelcast versions in parallel
#

set -e

version_1=maven=3.7.1
version_2=maven=3.7

# start the coordinator in remote mode
coordinator --monitorPerformance

coordinator-remote install ${version_1}
coordinator-remote install ${version_2}

# start a member with 1 version
coordinator-remote worker-start --count 1 --versionSpec ${version_1}
# start a member with the othr version
coordinator-remote worker-start --count 1 --versionSpec ${version_2}

# and run some test to see if test works fine.
# if there is a failure, this command will fail.
coordinator-remote run-start --duration 10m atomiclong.properties

# shut everything down
coordinator-remote stop
