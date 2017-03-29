#!/bin/bash
#
# This script is executed after the Coordinator completes executing a test-suite
#
# This script can be used for post processing, e.g. HDR file conversion.
# This script can be copied into the working directory and modified.
#

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

    # copy the files
    rsync --copy-links -avv -e "ssh ${SSH_OPTIONS}" $SIMULATOR_USER@$agent:$download_path $root_dir

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
            mv "$worker_dir"/* $target_dir
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

function postprocess_hdr(){
    session_dir=$1

    echo "[INFO]     Postprocessing HDR-histogram started..."

    # merge all hdr files of each member into a hdr file which gets stored in the target_directory
    probes=($(ls -R ${session_dir} | grep .hdr | sort | uniq))
    for probe in "${probes[@]}"
    do
        echo "Merging $probe"

        hdr_files=($(find ${session_dir} | grep ${probe}))

        java -cp "${SIMULATOR_HOME}/lib/*" com.hazelcast.simulator.utils.HistogramLogMerger \
            "${session_dir}/${probe}" "${hdr_files[@]}"
    done

    # convert all hdr files to hgrm files so they can easily be plot using
    # http://hdrhistogram.github.io/HdrHistogram/plotFiles.html
    hdr_files=($(find "${session_dir}" -name *.hdr))
    echo HDR FIles $hdr_files

    for hdr_file in "${hdr_files[@]}"
    do
        file_name="${hdr_file%.*}"
        java -cp "${SIMULATOR_HOME}/lib/*"  com.hazelcast.simulator.utils.SimulatorHistogramLogProcessor \
            -i ${hdr_file} \
            -o ${file_name} \
            -outputValueUnitRatio 1000

        mv "${file_name}.hgrm" "${file_name}.hgrm.bak"

        java -cp "${SIMULATOR_HOME}/lib/*"  com.hazelcast.simulator.utils.SimulatorHistogramLogProcessor \
            -csv \
            -i ${hdr_file} \
            -o ${file_name} \
            -outputValueUnitRatio 1000

        mv "${file_name}.hgrm.bak" "${file_name}.hgrm"
    done

    echo "[INFO]     Postprocessing HDR-histogram completed"
}

function postprocess_gclog(){
    session_dir=$1

    echo "[INFO]     Postprocessing GC-logs started..."

    # Conversion of gc.log to gc.csv
    gc_logs=($(find ${session_dir} -name gc.log))
    for gc_log in "${gc_logs[@]}"
    do
        dir=$(dirname $gc_log)
        gc_csv="$dir/gc.csv"

        java -jar "${SIMULATOR_HOME}/lib//gcviewer-1.35-SNAPSHOT.jar"  $gc_log $gc_csv -t CSV_FULL
    done

    echo "[INFO]     Postprocessing GC-logs completed"
}

function postprocess(){
    echo "[INFO]Postprocessing started..."

    for file in .* *;
    do
        if [ "$file" = "." ] || [ "$file" = ".." ] || [ "$file" = "logs" ] || [ -f "$file" ];
        then
            continue
        fi

        if [ "$session_id" = "*" ] || [ "$session_id" = "$file" ];
        then
            postprocess_hdr "$root_dir/$file"
            postprocess_gclog "$root_dir/$file"
        fi
    done

    echo "[INFO]Postprocessing completed"
}

echo "Sessionid [$session_id]"
echo "Agents [$agents]"
echo "Root directory [$root_dir]"

download
postprocess


