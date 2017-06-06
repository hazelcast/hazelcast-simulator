#!/usr/bin/env bash

# This script is executed on a freshly created node and provides the ability to
# e.g. install software, change settings etc. If a file 'init.sh' is created in
# the working directory, it will be used instead.
#
# NOTE: The following variables will be replaced with its proper values:
# + ${version} -> the Simulator version
# * ${user} -> the user name
# * ${cloudprovider} -> the used cloud provider

set -e
#set -x

function installPackage {
    PACKAGE=$1

    if hash ${PACKAGE} 2>/dev/null; then
        echo "$PACKAGE already installed"
        return 0
    fi

    CAN_I_RUN_SUDO=$(sudo -n uptime 2>&1 | grep "load" | wc -l)
    if [ ${CAN_I_RUN_SUDO} -le 0 ]; then
        echo "Cannot install '$PACKAGE', since '$USER' is not allowed to use sudo!"
        return 1
    fi

    if hash apt-get 2>/dev/null; then
        echo "apt-get is available!"
        sudo apt-get update
        sudo apt-get install -y ${PACKAGE}
    elif hash yum 2>/dev/null; then
        echo "yum is available!"
        sudo yum -y install ${PACKAGE}
    else
        echo "Cannot install '$PACKAGE', since apt-get AND yum are not available!"
        return 1
    fi
}

function mount_ephemeral(){
    METADATA_URL_BASE="http://169.254.169.254/2012-01-12"

    root_drive=`df -h | grep -v grep | awk 'NR==2{print $1}'`

    if [ "$root_drive" == "/dev/xvda1" ]; then
        echo "Detected 'xvd' drive naming scheme (root: $root_drive)"
        DRIVE_SCHEME='xvd'
    else
        echo "Detected 'sd' drive naming scheme (root: $root_drive)"
        DRIVE_SCHEME='sd'
    fi

    # figure out how many ephemerals we have by querying the metadata API, and then:
    #  - convert the drive name returned from the API to the hosts DRIVE_SCHEME, if necessary
    #  - verify a matching device is available in /dev/
    drives=""
    ephemeral_count=0
    ephemerals=$(curl --silent $METADATA_URL_BASE/meta-data/block-device-mapping/ | grep ephemeral)
    for e in $ephemerals; do
        echo "Probing $e .."
        device_name=$(curl --silent $METADATA_URL_BASE/meta-data/block-device-mapping/$e)
        # might have to convert 'sdb' -> 'xvdb'
        device_name=$(echo $device_name | sed "s/sd/$DRIVE_SCHEME/")
        device_path="/dev/$device_name"

        # test that the device actually exists since you can request more ephemeral drives than are available
        # for an instance type and the meta-data API will happily tell you it exists when it really does not.
        if [ -b $device_path ]; then
            echo "Detected ephemeral disk: $device_path"
            drives="$drives $device_path"
            ephemeral_count=$((ephemeral_count + 1 ))
        else
            echo "Ephemeral disk $e, $device_path is not present. skipping"
        fi
    done

    if [ "$ephemeral_count" = 0 ]; then
        echo "No ephemeral disk detected. exiting"
        return
    fi
}

installPackage dstat
installPackage curl

# Fix for a bug in an old Kernel on EC2 instances.
if [[ "${CLOUD_PROVIDER}" == "aws-ec2" ]]; then
    ver=$(awk -F. '{printf("%d%02d",$1,$2)}' <<< $(uname -r))
    if [ ${ver} -lt 319 ]; then
        echo 'Use Linux kernel 3.19+ when running Hazelcast on AWS'
        echo 'Applying fix: "sudo ethtool -K eth0 sg off"'
        sudo ethtool -K eth0 sg off
    fi

    mount_ephemeral

    # The following code is only executed on EC2.
    # By default the ~ directory is mapped to the / drive and this drive is quite small.
    # This is a problem with e.g. heap dumps, since they can't be created even if there
    # is enough space in the ephemeral drive. The following script maps the workers dir
    # to the ephemeral drive.
    if [ -d /mnt/ephemeral ] ; then
        if [ -d /mnt/ephemeral/workers ] ; then
            echo "[/mnt/ephemeral/workers] already exists on ephemeral drive"
        else
            echo "[/mnt/ephemeral/] exists, creating [/mnt/ephemeral/workers]"
            rm -fr hazelcast-simulator-${SIMULATOR_VERSION}/workers
            sudo mkdir /mnt/ephemeral/workers
            sudo chown -R ${SIMULATOR_USER} /mnt/ephemeral/workers/
            ln -s /mnt/ephemeral/workers/ hazelcast-simulator-${SIMULATOR_VERSION}/workers
        fi
    else
        echo "[/mnt/ephemeral/] is not found. Skip linking workers to ephemeral drive."
    fi
fi


