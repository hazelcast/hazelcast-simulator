#!/bin/bash

# This script is executed after the coordinator completes executing a test-suite and optionally has downloaded the artifacts.
# It will be executed 'locally'; on the machine that ran the executor, and not remotely.
#
# This script can be used for post processing e.g. HDR file conversion.
#
# This script can be copied into the working directory and modified.


# exit on failure
set -e

dir=$1
if [ ! -d "${dir}" ]; then
  echo Directory $dir does not exist.
  exit 1
fi

# Convert all hdr files to hgrm files so they can easily be plot using
# http://hdrhistogram.github.io/HdrHistogram/plotFiles.html
hdr_files=($(find $dir | grep .hdr))
for hdr_file in "${hdr_files[@]}"
do
        # prevent getting *.hdr as result in case of empty directory
        file_name="${hdr_file%.*}"

        classpath=$(ls $SIMULATOR_HOME/lib/HdrHistogram*)
        java -cp ${classpath}  org.HdrHistogram.HistogramLogProcessor -i ${hdr_file} -o ${file_name}
done

