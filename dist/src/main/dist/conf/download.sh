#!/bin/bash
#
# This script is executed to download all the artifacts.

# exit on failure
set -e
# printing the command being executed (useful for debugging)
#set -x

# the directory where the downloaded session(s) are going to be stored.
root_dir=$1
# the session id; could be a * om case everything needs to be downloaded
session_id=$2
# comma separated list of agent ip addresses
agents=$3


# Downloads the files from the remote machines.
function download_remote(){
    agent=$1

    echo "[INFO]    Download from $agent started"

    if [ "$session_id" = "*" ] ; then
         download_path="hazelcast-simulator-$SIMULATOR_VERSION/workers/"
    else
         download_path="hazelcast-simulator-$SIMULATOR_VERSION/workers/$session_id"
    fi

    # mask the license key in the benchmark result before downloading
    ssh ${SSH_OPTIONS} $SIMULATOR_USER@$agent "find ${download_path} -type f -name parameters -exec \
        sed -i 's/LICENCE_KEY=.*/LICENSE_KEY=###MASKED###/gi' {} \; || true"
    ssh ${SSH_OPTIONS} $SIMULATOR_USER@$agent "find ${download_path} -type f -name \"*.xml\" -exec \
        sed -i 's/<license-key>.*<\/license-key>/<license-key>###MASKED###<\/license-key>/gi' {} \; || true"

    # copy the files
    # we exclude the uploads directory because it could be very big e.g jars
    rsync --copy-links -avvz --compress-level=9 -e "ssh ${SSH_OPTIONS}" --exclude 'upload' $SIMULATOR_USER@$agent:$download_path $root_dir

    # delete the files on the agent (no point in keeping them around if they are already copied locally)
    if [ "$session_id" = "*" ] ; then
        ssh ${SSH_OPTIONS} $SIMULATOR_USER@$agent "rm -fr $download_path/*"
    else
        ssh ${SSH_OPTIONS} $SIMULATOR_USER@$agent "rm -fr $download_path"
    fi

    echo "[INFO]    Download from $agent completed"
}

# 'Downloads' the files from the local simulator/workers
function download_local(){
    echo "[INFO]Downloading from local machine...."

    workers_dir="${SIMULATOR_HOME}/workers"

    for worker_dir in $workers_dir/*;
    do
        worker_dir_name=$(basename "$worker_dir")
        if [ "$session_id" = "*" ] || [ "$session_id" = "$worker_dir_name" ];
        then
            target_dir="$root_dir/$worker_dir_name"
            mkdir -p $target_dir

            # mask the license key in the benchmark result before moving to the target dir
            find $worker_dir -type f -name parameters -exec
                sed -i 's/LICENCE_KEY=.*/LICENSE_KEY=###MASKED###/gi' {} \;
            find $worker_dir -type f -name "*.xml" -exec
                sed -i 's/<license-key>.*<\/license-key>/<license-key>###MASKED###<\/license-key>/gi' {} \;

            mv "$worker_dir"/* $target_dir

            # since we filter out the upload for a remote worker, lets do the same for local.
            rm -fr $target_dir/upload/

            mv ./agent.err $target_dir || true
            mv ./agent.out $target_dir || true
        fi
    done

    echo "[INFO]Download on local machine completed"
}

function download(){
    if [ "$CLOUD_PROVIDER" = "local" ]; then
        download_local
    else
        echo "[INFO]Download from remote machines (can take some time), sessionid [$session_id]..."

        for agent in ${agents//,/ } ; do
            download_remote ${agent} &
        done

        # wait for all downloads to complete
        wait

        echo "[INFO]Download completed"
    fi
}


echo "Sessionid [$session_id]"
echo "Agents [$agents]"
echo "Root directory [$root_dir]"

download


