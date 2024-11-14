#!/bin/bash

#
# Script to start up a Simulator Worker.
#
# To customize the behavior of the Worker, including Java configuration, copy this file into the 'work dir' of Simulator.
# See the end of this file for examples for different profilers.
#

# Enable strict error handling
set -euo pipefail

# printing the command being executed (useful for debugging)
#set -x

# Redirecting output and error to log files with timestamps
exec > >(tee -a worker.out) 2> >(tee -a worker.err >&2)

log() {
    local level="$1"
    shift
    echo "$(date '+%Y-%m-%d %H:%M:%S') [$level] $*"
}

# Read the parameters file and add it to the environment
while IFS='=' read -r key value; do
    export "$key"="$value"
done < "parameters"

validate_mount_params() {
    if [[ -n "${mount_path:-}" && -z "${mount_volume:-}" ]]; then
        log "ERROR" "mount_path is set but mount_volume is not set."
        exit 1
    fi

    if [[ -z "${mount_path:-}" && -n "${mount_volume:-}" ]]; then
        log "ERROR" "mount_volume is set but mount_path is not set."
        exit 1
    fi
}

validate_mount_path_safety() {
    local path="$1"
    case "$path" in
        / | /home | /var | /etc)
            log "WARNING" "mount_path $path is not allowed. Aborting."
            exit 1
            ;;
    esac
}

mount_persistence_volume() {
    log "INFO" "Setting up $mount_path for persistence volume..."

    # Unmount volume if mounted elsewhere
    if findmnt -S "$mount_volume" | grep -qv "$mount_path"; then
        log "INFO" "Unmounting $mount_volume from previous mount point..."
        sudo umount "$mount_volume" || { log "ERROR" "Failed to unmount $mount_volume"; exit 1; }
    fi

    if ! blkid "$mount_volume" | grep -q 'TYPE="xfs"'; then
        log "INFO" "Creating XFS filesystem on $mount_volume..."
        sudo mkfs.xfs -f "$mount_volume" || { log "ERROR" "Failed to format $mount_volume"; exit 1; }
    else
        log "INFO" "$mount_volume already has an XFS filesystem."
    fi

    # Create mount directory if it doesn't exist
    if [[ ! -d "$mount_path" ]]; then
        log "INFO" "Creating mount directory $mount_path..."
        sudo mkdir -p "$mount_path" || { log "ERROR" "Failed to create directory $mount_path"; exit 1; }
    fi

    log "INFO" "Setting ownership and permissions for $mount_path..."
    sudo chown "$members_user:$members_user" "$mount_path" || { log "ERROR" "Failed to change ownership of $mount_path"; exit 1; }
    sudo chmod 755 "$mount_path" || { log "ERROR" "Failed to set permissions for $mount_path"; exit 1; }

    log "INFO" "Mounting $mount_volume to $mount_path..."
    sudo mount "$mount_volume" "$mount_path" || { log "ERROR" "Failed to mount $mount_volume to $mount_path"; exit 1; }

    if mountpoint -q "$mount_path"; then
        log "INFO" "Successfully mounted $mount_volume to $mount_path."
        sudo chown "$members_user:$members_user" "$mount_path" || { log "ERROR" "Failed to change ownership of mounted filesystem"; exit 1; }
    else
        log "ERROR" "Mounting $mount_volume to $mount_path failed."
        exit 1
    fi
}

handle_persistence_volume() {
    # Validate that if one of mount_path or mount_volume is set, the other must be as well
    validate_mount_params

    if [[ -n "${mount_path:-}" && -n "${mount_volume:-}" ]]; then
        # Ensure mount_path is safe
        validate_mount_path_safety "$mount_path"

        # Check if mount_path is already a mount point
        if mountpoint -q "$mount_path"; then
            log "INFO" "Clearing contents of $mount_path directory..."
            sudo rm -rf "${mount_path:?}/"*
        else
            # Proceed with mounting
            mount_persistence_volume
        fi
    else
        log "INFO" "No persistence volume parameters provided. Skipping mounting process."
    fi
}

# Handle persistence volume mounting for members only
if [[ "${WORKER_TYPE}" = "member" ]]; then
  handle_persistence_volume
fi

# If you want to be sure that you have the right governor installed; uncomment
# the following 3 lines. They will force the right governor to be used.
#old_governor=$(sudo cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
#echo old_governor: $old_governor
#sudo cpupower frequency-set -g performance

# If you have a specific java version you want to use, uncomment the following lines
# and configure the path to the version to use.
#JAVA_HOME=~/java/jdk1.8.0_121
#PATH=$JAVA_HOME/bin:$PATH

JVM_ARGS="-Dlog4j2.configurationFile=log4j.xml"

if [ "${WORKER_TYPE}" = "member" ]; then
    JVM_OPTIONS=$member_args
else
    JVM_OPTIONS=$client_args
fi

# Include the member/client-worker jvm options
JVM_ARGS="$JVM_OPTIONS $JVM_ARGS"

MAIN=com.hazelcast.simulator.worker.Worker

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
# OpenOnload
#########################################################################
#
# The network stack for Solarflare network adapters (new lab).
#
# onload --profile=latency java -classpath $CLASSPATH $JVM_ARGS $MAIN
#
