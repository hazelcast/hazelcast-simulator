#!/usr/bin/env bash

# exit on error
set -e

image_width=1280
image_height=720

# TODO:
# - attractive plots
#           http://stackoverflow.com/questions/10496469/how-do-i-enable-the-pngcairo-terminal-in-gnuplot
# - clients and members (we don't want to see members in the throughput in case of client test)
# - generate 2 size pictures small/large
# - cut of parts of the plot which are after completion or before start.
# - dstat_plot_probe y format.
# - latency 75%
# - latency 95%
# - latency 98%
# - latency 99%
# - latency 99.9%
# - latency 99.99%
# - latency max
# - latency min
# - latency mean
# - control output directory for report.
# - better colours
# - graphs from same benchmark, same color.
# - local mode; files are not downloaded in right directory.
# - check if gnuplot is installed; otherwise fail fast. Also make reference to brew.
# - handle dstats missing
# - the aggregated per member hdr file start time broken
# - the aggregated per member hdr-history file has only a single item
# - time in the hdr file; should be expressed in micro's instead of milli's with a comma?

# DONE
# - hdr history file only has 1 item
# - hdr history file doesn't have timestamp set correctly
# - hdr history file should be cvs
# - latency
# - total throughput shuold be called aggregated
# - time of throughput per machine is off
# - problem with dstats file and performance.csv; the header causes the time offset calculation to be a problem
# - comparison between 2 runs
# - performance comparison aggregated throughput
# - performance comparison aggregated dstat
# - the names in the legend are 'file using 0...' ugly!
# - performance comparison per member throughput
# - performance comparison per member dstat
# - converted stat calculation to function call
# - statistics: http://unix.stackexchange.com/questions/13731/is-there-a-way-to-get-the-min-max-median-and-average-of-a-list-of-numbers-in
# - time should be beginning from 0 and not from a real time
# - generate html
# - time on throughput calculation not correct.
# - throughput should not use scientific notation
# - plot_throughput should get files as variable
# - argument for input directory
# - merged throughput
# - time on throughput_total calculation not correct.
# - the minimum in statistics is not there.
# - throughput individual now has correct member info.
# - flipped aggregated and non aggregated.

directories=("$@")

for dir in "${directories[@]}"
do
    if [ ! -d "$dir" ]; then
        echo Directory $dir does not exist
        exit 1
    fi

    echo "$var"
done

dir=$1

dstat_file_array=($(find $dir | grep dstat.csv))

# ================================== gnuplot tools =============================

plot_start(){
    rm -fr gnu.plot
}

plot_complete(){
    gnuplot gnu.plot
}

plot(){
    command=$1
    echo "$command" >> gnu.plot
}

# ================================== html tools =============================

html_title(){
    title=$1
    html "<h1>$title</h1>"
}

html_img(){
     img=$1
     html "<img src=\"$img\" style=\"width:640px;height:480px;\" onclick=\"window.open(this.src)\">"
}

html_head(){
    rm -fr report.html
    html "<html>"
    html "<body>"
}

html_end(){
    html "</body>"
    html "</html>"
}

html(){
    text=$1
    echo "$text" >> report.html
}

# ============================================================================

html_throughput(){
    dir=$1

    plot_start
    plot "set style data lines"
    plot 'set datafile separator ","'
    plot "set terminal png size $image_width,$image_height"
    plot "set grid"
    plot "set key below"
    plot "set xlabel 'Time seconds'"
    plot "set xdata time"
    plot "set timefmt \"%s\""
    plot "offset = 0"
    plot "t0(x)=(offset=(\$0==0) ? x : offset, x - offset)"
    plot "set title 'Throughput Per Machine'"
    plot "set ylabel 'operations/second'"
    plot "set format y '%.0f'"
    plot "set output 'images/throughput_per_worker.png'"
    plot "plot \\"
    for dir in "${directories[@]}"
    do
        files=($(find $dir | grep performance.csv))
        benchmark_name=$(basename $dir)

        for file in "${files[@]}"
        do
            if [ ${#directories[@]} -eq 1 ]; then
                worker_name=$(basename $(dirname $file))
            else
                worker_name="${benchmark_name}#$(basename $(dirname $file))"
            fi
            # we need to skip the first line because it contains header info and the time logic will choke on it.
            plot "   \"$file\" every ::2 using (t0(timecolumn(1))):5  title '$worker_name'   noenhanced with lines, \\"
        done
    done
    plot_complete

    plot_start
    plot "set style data lines"
    plot 'set datafile separator ","'
    plot "set terminal png size $image_width,$image_height"
    plot "set grid"
    plot "set key below"
    plot "set xlabel 'Time seconds'"
    plot "set xdata time"
    plot "set timefmt \"%s\""
    plot "offset = 0"
    plot "t0(x)=(offset=(\$0==0) ? x : offset, x - offset)"
    plot "set title 'Throughput Aggregated'"
    plot "set ylabel 'operations/second'"
    plot "set format y '%.0f'"
    plot "set output 'images/throughput_aggregated.png'"
    plot "plot \\"
    for dir in "${directories[@]}"
    do
        files=($(find $dir | grep performance.csv))
        benchmark_name=$(basename $dir)
        aggregated_file="${benchmark_name}_throughput_aggregated.tmp"

        # merge the operations/second column of all files and write that aggregated_file
        awk -F ',' '
        {
            sum[FNR]+=$5;
            b[FNR]++;
            name[FNR]=$1;
        } END {
            for(i=1;i<=FNR;i++)
                printf "%s, %.2f\n", name[i],sum[i];
            }' "${files[@]
        }" > "${aggregated_file}"

        # cut of first line because it contains the headers.
        sed -i -e 1,1d "${aggregated_file}"

        plot " \"${aggregated_file}\" using (t0(timecolumn(1))):2 title '${benchmark_name}' with lines , \\"
    done
    plot_complete


    html_title "Throughput"
    html_img "images/throughput_aggregated.png"
    html_img "images/throughput_per_worker.png"

    html "<table border=\"2\">"
    html "<tr><th>Average</th><th>Median</th><th>Min</th><th>Max</th></tr>"
    for dir in "${directories[@]}"
    do
        benchmark_name=$(basename $dir)
        aggregated_file="${benchmark_name}_throughput_aggregated.tmp"
        html_stats "${aggregated_file}" 2
    done
    html "</table>"

    rm -fr *.tmp
}

html_stats()
{
    csv_file=$1
    column=$2

    # first we write the column into a file.
    awk -v column=$column  '{print $column}' ${csv_file} > tmp

    # then we calculate the results.
    result=$( cat tmp | sort -n | awk '
        BEGIN {
            c = 0;
            sum = 0;
        }
        $1 ~ /^[0-9]*(\.[0-9]*)?$/ {
            a[c++] = $1;
        sum += $1;
        }
        END {
            mean = sum / c;
            if( (c % 2) == 1 ) {
                median = a[ int(c/2) ];
            } else {
                median = ( a[c/2] + a[c/2-1] ) / 2;
            }
            OFS="\t";
            print c, mean, median, a[0], a[c-1];
        }')

    rm tmp

    stats=($result)

    html "<tr><td>${stats[1]}</td><td>${stats[2]}</td><td>${stats[3]}</td><td>${stats[4]}</td></tr>"
}

html_latency(){
    plot_start
    plot "set terminal png size $image_width,$image_height"
    plot "unset xtics"
    plot "set logscale x"
    plot "set key top left"
    plot "set style line 1 lt 1 lw 3 pt 3 linecolor rgb \"red\""
    plot "set output 'images/latency.png'"
    plot "plot './xlabels.dat' with labels center offset 0, 1.5 point,\\"

    for dir in "${directories[@]}"
    do
        # look for all the hgrm in the root dir of the benchmark (so no member level)
        files=($(find $dir -maxdepth 1 | grep .hgrm))
        benchmark_name=$(basename $dir)

        for file in "${files[@]}"
        do
            worker_name=$(basename $(dirname $file))

            if [ ${#directories[@]} -eq 1 ]; then
                line_title="$worker_name"
            else
                line_title="${benchmark_name}_${worker_name}"
            fi

            plot "   \"$file\" using 4:1 with lines, \\"
        done
    done
    plot_complete

    html_title "Latency"
    html_img "images/latency.png"
}

plot_dstat_probe(){
    title=$1
    ylabel=$2
    image=$3
    column=$4

    plot_start
    plot "set style data lines"
    plot "set datafile separator \",\""
    plot "set terminal png size $image_width,$image_height"
    plot "set grid"
    plot "set key below"
    plot "set xlabel 'time seconds'"
    plot "set xdata time"
    plot "set timefmt \"%s\""
    plot "offset = 0"
    plot "t0(x)=(offset=(\$0==0) ? x : offset, x - offset)"
    plot "set title '$title'"
    plot "set ylabel '$ylabel'"
    plot "set output 'images/${image}.png'"
    plot "plot \\"
    for dir in "${directories[@]}"
    do
        files=($(find $dir | grep dstat.csv))
        benchmark_name=$(basename $dir)

        for file in "${files[@]}"
        do
            worker_name=$(basename $(dirname $file))

            if [ ${#directories[@]} -eq 1 ]; then
                line_title="$worker_name"
            else
                line_title="${benchmark_name}_${worker_name}"
            fi

            # The worker file contains 2 columsn, 1 the timestap, 2 the selected column from the dstats file
            worker_file="${benchmark_name}-${worker_name}.single.tmp"

            # select the first (time) and desired column and write it to tmp
            awk -F "," -v column=$column '{printf "%s,%s\n",$1,$column}' $file > "$worker_file"

            # remove the first 7 lines, since they contain header info
            sed -i -e 1,7d "$worker_file"

            # and then we plot worker_file
            plot "   \"$worker_file\" using (t0(timecolumn(1))):2 title '$line_title' noenhanced with lines, \\"
        done
    done
    plot_complete

    # now we do the aggregation.
    plot_start
    plot "set style data lines"
    plot "set datafile separator \",\""
    plot "set terminal png size $image_width,$image_height"
    plot "set grid"
    plot "set key below"
    plot "set xlabel 'time seconds'"
    plot "set xdata time"
    plot "set timefmt \"%s\""
    plot "offset = 0"
    plot "t0(x)=(offset=(\$0==0) ? x : offset, x - offset)"
    plot "set title '$title Aggregated'"
    plot "set ylabel '$ylabel'"
    plot "set output 'images/${image}_aggregated.png'"
    plot "plot \\"
    for dir in "${directories[@]}"
    do
        benchmark_name=$(basename $dir)
        aggregated_file="${benchmark_name}_aggregated.tmp"

        files=($(find ${benchmark_name}-*.single.tmp))

        # merge the operations/second column of all files and write that to aggregated file
        awk -F ',' '
        {
            sum[FNR]+=$2;
            b[FNR]++;
            name[FNR]=$1;
        } END {
            for(i=1;i<=FNR;i++)
                printf "%s, %.2f\n", name[i],sum[i];
            }' "${files[@]
        }" > ${aggregated_file}

        plot " \"${aggregated_file}\" using (t0(timecolumn(1))):2 title '${benchmark_name}' with lines , \\"
    done
    plot_complete

    html "<table border=\"2\">"
    html "<tr><th>Average</th><th>Median</th><th>Min</th><th>Max</th></tr>"
    for dir in "${directories[@]}"
    do
        benchmark_name=$(basename $dir)
        aggregated_file="${benchmark_name}_aggregated.tmp"
        html_stats "${aggregated_file}" 2
    done
    html "</table>"

    rm -fr *.tmp
}

html_dstats()
{
    ###################################################
    #               CPU
    ###################################################

    html_title "Total Cpu Usage"

    html_img "images/cpu_user_aggregated.png"
    html_img "images/cpu_user.png"
    plot_dstat_probe "CPU User %" "CPU User %" "cpu_user" 6

    html_img "images/cpu_system_aggregated.png"
    html_img "images/cpu_system.png"
    plot_dstat_probe "CPU System %" "CPU System %" "cpu_system" 7

    html_img "images/cpu_idle_aggregated.png"
    html_img "images/cpu_idle.png"
    plot_dstat_probe "CPU Idle %" "CPU Idle %" "cpu_idle" 8

    html_img "images/cpu_wait_aggregated.png"
    html_img "images/cpu_wait.png"
    plot_dstat_probe "CPU Wait %" "CPU Wait %" "cpu_wait" 9

    ###################################################
    #               memory
    ###################################################
    html_title "Memory Usage"

    html_img "images/memory_used_aggregated.png"
    html_img "images/memory_used.png"
    plot_dstat_probe "Memory Used" "Memory Used" "memory_used" 2

    html_img "images/memory_cached_aggregated.png"
    html_img "images/memory_cached.png"
    plot_dstat_probe "Memory Buffered" "Memory Buffered" "memory_buffered" 3

    html_img "images/memory_buffered_aggregated.png"
    html_img "images/memory_buffered.png"
    plot_dstat_probe "Memory Cached" "Memory Cached" "memory_cached" 4

    html_img "images/memory_free_aggregated.png"
    html_img "images/memory_free.png"
    plot_dstat_probe "Memory Free" "Memory Free" "memory_free" 5

    ###################################################
    #               Interrupts
    ###################################################

    html_title "Interrupts"

    html_img "images/cpu_hardware_interrupts_aggregated.png"
    html_img "images/cpu_hardware_interrupts.png"
    plot_dstat_probe "CPU Hardware Interrupts" "CPU Hardware Interrupts/sec" "cpu_hardware_interrupts" 10

    html_img "images/cpu_software_interrupts_aggregated.png"
    html_img "images/cpu_software_interrupts.png"
    plot_dstat_probe "CPU Software Interrupts" "CPU Software Interrupts/sec" "cpu_software_interrupts" 11

    ###################################################
    #               Interrupts
    ###################################################

    html_title "Network"

    html_img "images/net_send_aggregated.png"
    html_img "images/net_send.png"
    plot_dstat_probe "Net Receive" "Receiving/second" "net_receive" 14

    html_img "images/net_receive_aggregated.png"
    html_img "images/net_receive.png"
    plot_dstat_probe "Net Send" "Sending/second" "net_send" 15

    ###################################################
    #               System
    ###################################################

    html_title "System"

    html_img "images/system_interrupts_aggregated.png"
    html_img "images/system_interrupts.png"
    plot_dstat_probe "System Interrupts" "System Interrupts/second" "system_interrupts" 17

    html_img "images/system_context_switches_aggregated.png"
    html_img "images/system_context_switches.png"
    plot_dstat_probe "System Context Switches" "System Context Switches/sec" "system_context_switches" 18

    ###################################################
    #               Disk
    ###################################################

    html_title "Disk"

    html_img "images/disk_read_aggregated.png"
    html_img "images/disk_read.png"
    plot_dstat_probe "Disk Read" "" "disk_read" 17

    html_img "images/disk_write.png"
    html_img "images/disk_write_aggregated.png"
    plot_dstat_probe "Disk Write" "" "disk_write" 18

    ###################################################
    #               Paging
    ###################################################

    html_title "Paging"

    html_img "images/page_in_aggregated.png"
    html_img "images/page_in.png"
    plot_dstat_probe "Page In" "Page in" "page_in" 12

    html_img "images/page_out_aggregated.png"
    html_img "images/page_out.png"

    plot_dstat_probe "Page Out" "Page out" "page_out" 13
}

mkdir -p images

html_head

html_throughput

html_latency

html_dstats

html_end