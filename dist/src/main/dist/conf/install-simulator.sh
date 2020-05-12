#!/bin/bash

# exit on failure
set -e

agent=$1

uploadLibraryJar(){
    pattern=$1
    src="$SIMULATOR_HOME/lib/$pattern"
    rsync --checksum -avv -L -e "ssh $SSH_OPTIONS" $src $SIMULATOR_USER@$agent:hazelcast-simulator-$SIMULATOR_VERSION/lib
}

uploadTestLibJar(){
    pattern=$1
    src="$SIMULATOR_HOME/drivers/driver-$VENDOR/$pattern"
    rsync --checksum -avv -L -e "ssh $SSH_OPTIONS" $src $SIMULATOR_USER@$agent:hazelcast-simulator-$SIMULATOR_VERSION/drivers/driver-$VENDOR/
}

uploadToRemoteSimulatorDir(){
    src=$1
    target=$2
    rsync --checksum -avv -L -e "ssh $SSH_OPTIONS" $src $SIMULATOR_USER@$agent:hazelcast-simulator-$SIMULATOR_VERSION/$target
}

# purge Hazelcast JARs
ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm -fr hazelcast-simulator-$SIMULATOR_VERSION/vendor-lib/ || true"
ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "rm -fr hazelcast-simulator-$SIMULATOR_VERSION/user-lib/ || true"

ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "mkdir -p hazelcast-simulator-$SIMULATOR_VERSION/lib/"
ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "mkdir -p hazelcast-simulator-$SIMULATOR_VERSION/user-lib/"

#upload Simulator JARs
uploadLibraryJar "simulator-*"

# we don't copy all JARs to the agent to increase upload speed
#activemq libraries
uploadLibraryJar "activemq-broker*"
uploadLibraryJar "activemq-client*"
uploadLibraryJar "geronimo-jms*"
uploadLibraryJar "geronimo-j2ee*"
uploadLibraryJar "slf4j-api*"
uploadLibraryJar "hawtbuf-*"
uploadLibraryJar "affinity*"
uploadLibraryJar "jna*"

uploadLibraryJar "cache-api*"
uploadLibraryJar "commons-codec*"
uploadLibraryJar "commons-lang3*"
uploadLibraryJar "freemarker*"
uploadLibraryJar "gson-*"
uploadLibraryJar "HdrHistogram-*"
uploadLibraryJar "jopt*"
uploadLibraryJar "junit*"
uploadLibraryJar "log4j*"
uploadLibraryJar "slf4j-log4j12-*"

# hack to get hz enterprise working
if [ "$VENDOR" = "hazelcast-enterprise4" ]; then
    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "mkdir -p hazelcast-simulator-$SIMULATOR_VERSION/drivers/driver-hazelcast4"
    rsync --checksum -avv -L -e "ssh $SSH_OPTIONS" $SIMULATOR_HOME/drivers/driver-hazelcast4/* \
            $SIMULATOR_USER@$agent:hazelcast-simulator-$SIMULATOR_VERSION/drivers/driver-hazelcast4/
else
    ssh $SSH_OPTIONS $SIMULATOR_USER@$agent "mkdir -p hazelcast-simulator-$SIMULATOR_VERSION/drivers/driver-$VENDOR"
    uploadTestLibJar "*"
fi

#

# upload remaining files
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/bin/" "bin"
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/conf/" "conf"
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/jdk-install/" "jdk-install"
uploadToRemoteSimulatorDir "$SIMULATOR_HOME/user-lib/" "user-lib/"
