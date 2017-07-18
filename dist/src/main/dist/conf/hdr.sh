#!/bin/bash
#
# This script is executed as part of the report generation

# exit on failure
set -e
# printing the command being executed (useful for debugging)
#set -x

# the session id; could be a * om case everything needs to be downloaded
session_dir=$1

# merge all hdr files of each member into a hdr file which gets stored in the target_directory
probes=($(ls -R ${session_dir} | grep .hdr | sort | uniq))
for probe in "${probes[@]}"
do
    echo "Merging $probe"

    hdr_files=($(find ${session_dir} | grep ${probe}))
    echo "[INFO]          $probe"
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

    echo "[INFO]          $hdr_file"

    mv "${file_name}.hgrm.bak" "${file_name}.hgrm"
done
