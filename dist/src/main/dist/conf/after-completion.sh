#!/bin/bash
#
# This script is executed after the Coordinator completes executing a test-suite and optionally has downloaded the artifacts.
# It will be executed 'locally'; on the machine that ran the executor, and not remotely.
#
# This script can be used for post processing, e.g. HDR file conversion.
# This script can be copied into the working directory and modified.
#

# exit on failure
set -e
# printing the command being executed (useful for debugging)
#set -x

dir=$1
if [ ! -d "${dir}" ]; then
    echo "Directory $dir does not exist. Exiting silently."
    exit 0
fi

# merge all hdr files of each member into a hdr file which gets stored in the dir
probes=($(ls -R ${dir} | grep .hdr | sort | uniq))
for probe in "${probes[@]}"
do
    echo "Processing $probe"

    hdr_files=($(find ${dir} | grep ${probe}))

   java -cp "${SIMULATOR_HOME}/lib/*" com.hazelcast.simulator.utils.HistogramLogMerger "${dir}/${probe}" "${hdr_files[@]}"
done

# convert all hdr files to hgrm files so they can easily be plot using
# http://hdrhistogram.github.io/HdrHistogram/plotFiles.html
hdr_files=($(find ${dir} -name *.hdr))
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

# Conversion of gc.log to gc.csv
gc_logs=($(find ${dir} -name gc.log))
for gc_log in "${gc_logs[@]}"
do
    dir=$(dirname $gc_log)
    gc_csv="$dir/gc.csv"

    java -jar "${SIMULATOR_HOME}/lib//gcviewer-1.35-SNAPSHOT.jar"  $gc_log $gc_csv -t CSV_FULL
done

