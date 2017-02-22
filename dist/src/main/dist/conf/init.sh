#!/bin/sh

# This script is executed on a freshly created node and provides the ability to
# e.g. install software, change settings etc. If a file 'init.sh' is created in
# the working directory, that script will be used.



function install {
        PACKAGE=$1

        if hash ${PACKAGE} 2>/dev/null; then
            echo ${PACKAGE} already installed
            return 0
        fi

        if hash apt-get 2>/dev/null; then
            sudo apt-get update
            sudo apt-get install -y ${PACKAGE}
            echo apt-get available
        elif hash yum 2>/dev/null; then
            echo yum available
            sudo yum -y install ${PACKAGE}
        else
            echo apt-get/yum not available, not installing ${PACKAGE}
        fi
}

install dstat

# The following code is only executed on EC2.
# By default the ~ directory is mapped to the / drive and this drive is quite small.
# This is a problem with e.g. heap dumps, since they can't be created even if there
# is enough space in the ephemeral drive. The following script maps the workers dir
# to the ephemeral drive.
if [ -d /mnt/ephemeral ] ; then
    if [ -d /mnt/ephemeral/workers ] ; then
        echo "[/mnt/ephemeral/workers] already exist on ephemeral drive"
    else
        echo "[/mnt/ephemeral/] exists, creating [/mnt/ephemeral/workers]"
        rm -fr hazelcast-simulator-${version}/workers
        sudo mkdir /mnt/ephemeral/workers
        sudo chown -R ${user} /mnt/ephemeral/workers/
        ln -s /mnt/ephemeral/workers/ hazelcast-simulator-${version}/workers
     fi

    ver=$(awk -F. '{printf("%d%02d",$1,$2)}' <<< $(uname -r))
    if (( ${ver} < 319 )); then
        echo 'Use Linux kernel 3.19+ when running Hazelcast on AWS'
        echo 'Applying fix: "sudo ethtool -K eth0 sg off"'
        sudo ethtool -K eth0 sg off
    fi
else
    echo "[/mnt/ephemeral/] is not found. Skipping linking workers to ephemeral drive."
fi
