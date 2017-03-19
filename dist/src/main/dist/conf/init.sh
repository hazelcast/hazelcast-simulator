#!/usr/bin/env bash

# This script is executed on a freshly created node and provides the ability to
# e.g. install software, change settings etc. If a file 'init.sh' is created in
# the working directory, it will be used instead.
#
# NOTE: The following variables will be replaced with its proper values:
# + ${version} -> the Simulator version
# + all simulator properties

set -e
#set -x

function installPackage {
    PACKAGE=$1

    if hash ${PACKAGE} 2>/dev/null; then
        echo "$PACKAGE already installed"
        return 0
    fi

    if hash apt-get 2>/dev/null; then
        echo "apt-get is available!"
        sudo apt-get update
        sudo apt-get install -y ${PACKAGE}
    elif hash yum 2>/dev/null; then
        echo "yum is available!"
        sudo yum -y install ${PACKAGE}
    else
        echo "apt-get AND yum are not available!"
    fi
}

installPackage dstat


if [[ "${CLOUD_PROVIDER}" == "aws-ec2" ]]; then

    # we modify the behavior of the instance so it terminates on shutdown (normally it stops)
    EC2_INSTANCE_ID="`wget -q -O - http://instance-data/latest/meta-data/instance-id || die \"wget instance-id has failed: $?\"`"
    ec2-modify-instance-attribute --aws-access-key ${CLOUD_IDENTITY} \
                                  --aws-secret-key ${CLOUD_CREDENTIAL} \
        --instance-initiated-shutdown-behavior terminate \
        $EC2_INSTANCE_ID

    # Fix for a bug in an old Kernel on EC2 instances.
    ver=$(awk -F. '{printf("%d%02d",$1,$2)}' <<< $(uname -r))
    if [ ${ver} -lt 319 ]; then
        echo 'Use Linux kernel 3.19+ when running Hazelcast on AWS'
        echo 'Applying fix: "sudo ethtool -K eth0 sg off"'
        sudo ethtool -K eth0 sg off
    fi
fi

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
        rm -fr hazelcast-simulator-${version}/workers
        sudo mkdir /mnt/ephemeral/workers
        sudo chown -R ${SIMULATOR_USER} /mnt/ephemeral/workers/
        ln -s /mnt/ephemeral/workers/ hazelcast-simulator-${version}/workers
    fi
else
    echo "[/mnt/ephemeral/] is not found. Skip linking workers to ephemeral drive."
fi
