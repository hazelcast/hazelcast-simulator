#!/usr/bin/env bash

# This script is executed on a freshly created node and provides the ability to
# e.g. install software, change settings etc. If a file 'init.sh' is created in
# the working directory, it will be used instead.
#
# NOTE: We cannot use functions here, since the script directly executed on the shell.

if hash dstat 2>/dev/null; then
    echo "dstat already installed"
    return 0
fi

if hash apt-get 2>/dev/null; then
    echo "apt-get is available!"
    sudo apt-get update
    sudo apt-get install -y dstat
elif hash yum 2>/dev/null; then
    echo "yum is available!"
    sudo yum -y install dstat
else
    echo "apt-get AND yum are not available!"
fi

# Fix for a bug in an old Kernel on EC2 instances.
if [[ "$cloudprovider" == "aws-ec2" ]]; then
    ver=$(awk -F. '{printf("%d%02d",$1,$2)}' <<< $(uname -r))
    if (( ${ver} < 319 )); then
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
        sudo chown -R ${user} /mnt/ephemeral/workers/
        ln -s /mnt/ephemeral/workers/ hazelcast-simulator-${version}/workers
    fi
else
    echo "[/mnt/ephemeral/] is not found. Skip linking workers to ephemeral drive."
fi
