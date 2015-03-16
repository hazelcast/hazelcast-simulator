#!/bin/sh

# This script is executed on a freshly created node and provides the ability to
# e.g. install software, change settings etc. If a file 'init.sh' is created in
# the working directory, that script will be used.

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
        sudo chown -R ${user}  /mnt/ephemeral/workers/
        ln -s  /mnt/ephemeral/workers/ hazelcast-simulator-${version}/workers
     fi
else
    echo "[/mnt/ephemeral/] is not found. Skipping linking workers to ephemeral drive."
fi