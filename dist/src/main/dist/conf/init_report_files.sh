#!/bin/bash
#
# This script is executed as part of the report generation

# exit on failure
set -e
# printing the command being executed (useful for debugging)
#set -x

# the session id; could be a * om case everything needs to be downloaded
session_dir=$1
report_dir=$2
hdr_target_dir_name=$3
time_start_millis=$4
time_end_millis=$5

worker_dir_names=($(ls ${session_dir}))
# copy all the hdr files
for worker_dir_name in "${worker_dir_names[@]}"
do
    worker_dir=$session_dir/$worker_dir_name
    if [ -d "${worker_dir}" ] ; then
        mkdir -p $report_dir/tmp/$hdr_target_dir_name/$worker_dir_name
        cp $session_dir/$worker_dir_name/*.hdr $report_dir/tmp/$hdr_target_dir_name/$worker_dir_name/ || true
    fi
done

hdr_files=($(find "${report_dir}/tmp/$hdr_target_dir_name" -name *.hdr))
for hdr_file in "${hdr_files[@]}"
do
    echo "trimmig $hdr_file"
    java -cp "${SIMULATOR_HOME}/lib/*"  com.hazelcast.simulator.utils.HistogramTrimmer $hdr_file $time_start_millis $time_end_millis
done

# merge all hdr files of each driver into a hdr file which gets stored in the target_directory
probes=($(ls -R ${report_dir}/tmp/$hdr_target_dir_name | grep .hdr | sort | uniq))
for probe in "${probes[@]}"
do
    echo "Merging probe $probe"

    worker_dir_names=($(ls ${report_dir}/tmp/$hdr_target_dir_name))
    for worker_dir_name in "${worker_dir_names[@]}"
    do
        hdr_file="${report_dir}/tmp/$hdr_target_dir_name/${worker_dir_name}/${probe}"
        if [ -f $hdr_file ] ; then
            echo "found hdr_file: $hdr_file"
            hdr_files+=("$hdr_file" )
        fi
    done
    echo "${hdr_files[@]}"

    java -cp "${SIMULATOR_HOME}/lib/*" com.hazelcast.simulator.utils.HistogramLogMerger \
             "${report_dir}/tmp/$hdr_target_dir_name/${probe}" "${hdr_files[@]}" 2>/dev/null
done

# convert all hdr files to hgrm files so they can easily be plot using
# http://hdrhistogram.github.io/HdrHistogram/plotFiles.html
hdr_files=($(find "${report_dir}/tmp/$hdr_target_dir_name" -name *.hdr))
echo HDR Files $hdr_files
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

# copy performance.csv files
for worker_dir_name in "${worker_dir_names[@]}"
do
    worker_dir=$session_dir/$worker_dir_name
    if [ -d "${worker_dir}" ] ; then
         mkdir -p $report_dir/tmp/$hdr_target_dir_name/$worker_dir_name
         cp $session_dir/$worker_dir_name/performance*.csv $report_dir/tmp/$hdr_target_dir_name/$worker_dir_name/ || true
    fi
done

# generate the report.csv
hgrm_files=($(find "${report_dir}/tmp/$hdr_target_dir_name" -maxdepth 1 -name *.hgrm ))
echo HDR hgrm_files $hgrm_files
for hgrm_file in "${hgrm_files[@]}"
do
    java -cp "${SIMULATOR_HOME}/lib/*"  com.hazelcast.simulator.utils.ReportCsv $hgrm_file $report_dir $session_dir
done

# copy the dstats files
cp ${session_dir}/*_dstat.csv ${report_dir}/tmp/$hdr_target_dir_name


