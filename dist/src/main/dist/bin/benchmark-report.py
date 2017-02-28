#!/usr/bin/env python
# -*- coding: utf-8 -*-

# todo:
# - writing html
# - pruning of dstats to match running time
# - when comparing benchmarks; use 1 color for all plots from 1 benchmark
# - if no latency info is found; print warning
# - when not a lot of data points, them time issues in gnuplot (use WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS=default)
# - timeseries: avg
# - gnuplot y axis formatting; long numbers are unreadable because not dots or comma's
# - throughput per member in main output directory
# - default gnuplot colors stink; often they are not distinguishable
# - latency distribution doesn't show the percentiles; doesn't load xlabels.csv
#
# done:
# - better commandline help
#
# backlog
# - google chart option
# - svg option
# - latency per worker
# - option to plot with real time.
# - dstats merging for members?
# - cpu usage merging needs to be divided by number of agents.
# - option not to make low part of graph shrink
# - option to show real time

import csv
import os
import re
import tempfile
import argparse

parser = argparse.ArgumentParser(description='Creating a benchmark report from one or more benchmarks.')
parser.add_argument('benchmarks', metavar='B', nargs='+',
                    help='a benchmark to be used in the comparison')
#parser.add_argument('-r', '--realtime', default='report', help='print the real time of the datapoints.')
parser.add_argument('-o', '--output', nargs=1,
                    help='The output directory for the report. By default a report directory in the working directory is created.')

x = parser.parse_args()
args = x.benchmarks

if not x.output:
    output_dir = "report"
else:
    output_dir = x.output[0]

print("output directory '" + output_dir + "'")

# ================ html ========================

def html(line):
    reportFile.write(line + '\n')


def html_init():
    html("<html>")
    html("<html></head>")
    html("<body>")


def html_close():
    html("</body>")
    html("</html")
    reportFile.close()


def html_h1(title):
    html("<h2>" + title + "</h2>")


def html_h2(title):
    html("<h2>" + title + "</h2>")


def html_h3(title):
    html("<h3>" + title + "</h3>")


# ================ utils ========================

def dump(obj):
    for attr in dir(obj):
        print "obj.%s = %s" % (attr, getattr(obj, attr))



def ensure_dir(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)


# ================ plotting =========================

class Gnuplot:
    image_width = 1280
    image_height = 1024
    filepath = None
    ylabel = None
    is_bytes = None

    def __init__(self, directory, title):
        self.tmp = tempfile.NamedTemporaryFile(delete=False)
        self.title = title
        self.directory = directory
        self.ts_list = []
        self.titles = {}

    def _complete(self):
        self.tmp.flush()
        from os import system
        system('gnuplot ' + self.tmp.name)

    def _write(self, line):
        self.tmp.write(line + '\n')

    def add(self, ts, title=None):
        self.ts_list.append(ts)
        self.titles[ts] = title
        return self

    def plot(self):
        empty = True
        for ts in self.ts_list:
            if not ts.is_empty():
                empty = False
                break

        if empty:
            print("Skipping plot of " + self.title + "; timeseries are empty")
            return

        ts_first = self.ts_list[0]
        self.ylabel = ts_first.ylabel
        self.filepath = os.path.join(self.directory, ts_first.name + ".png")
        self.is_bytes = ts_first.is_bytes

        ensure_dir(self.directory)
        self.do_plot()

        print(self.filepath)

    def do_plot(self):
        raise NotImplementedError("Please Implement this method")


class TimeseriesGnuplot(Gnuplot):
    def __init__(self, directory, title):
        Gnuplot.__init__(self, directory, title)

    def do_plot(self):
        #self._write("unset autoscale y")
        self._write("set title '" + self.title + "'")
        self._write("set style data lines")
        self._write('set datafile separator ","')
        self._write("set terminal png size " + str(self.image_width) + "," + str(self.image_height))
        self._write("set grid")
        self._write("set key below")

        self._write("set xdata time")
        self._write("set timefmt \"%s\"")
        self._write("offset = 0")
        self._write("t0(x)=(offset=($0==0) ? x : offset, x - offset)")
        self._write("set xlabel 'Time minutes:seconds'")

        self._write("set ylabel '" + self.ylabel + "'")
        if self.is_bytes:
            # the problem here is that this is 1000 based; not 1024
            self._write("set format y '%.1s%cB'")

        #else:
        #    self._write("set format y '%.0f'")

        self._write("set output '" + self.filepath + "'")
        self._write("plot \\")

        for ts in self.ts_list:
            ts_file = ts.to_tmp_file()

            if len(self.ts_list) > 1:
                title = self.titles[ts]
                if not title:
                    title = ts.name
                title_str = "title \"" + title + "\" noenhanced"
            else:
                title_str = "title \"\""

            self._write("   \'" + ts_file.name + "\' using (t0(timecolumn(1))):2 " + title_str + ", \\")
        self._complete()

class LatencyDistributionGnuplot(Gnuplot):
    def __init__(self, directory, title):
        Gnuplot.__init__(self, directory, title)

    def do_plot(self):
        self._write("set datafile separator \",\"")
        self._write("set title '" + self.title + "'")
        self._write("set terminal png size " + str(self.image_width) + "," + str(self.image_height))
        self._write("set grid")
        self._write("unset xtics")
        self._write("set ylabel 'Latency (μs)'")
        self._write("set logscale x")
        self._write('set key top left')
        self._write("set style line 1 lt 1 lw 3 pt 3 linecolor rgb \"red\"")
        self._write("set output '" + self.filepath + "'")
        self._write("plot 'xlabels.csv' with labels center offset 0, 1.5 point,\\")
        for ts in self.ts_list:
            ts_file = ts.to_tmp_file()

            if len(self.ts_list) > 1:
                title = self.titles[ts]
                if not title:
                    title = ts.name
                title_str = "title \"" + title + "\" noenhanced"
            else:
                title_str = "title \"\""

            self._write("   \"" + ts_file.name + "\" using 1:2 " + title_str + " with lines, \\")

        self._complete()
        print(self.tmp.name)


class GoogleCharts:
    def __init__(self, ts, directory, title):
        self.title = title
        self.ts = ts
        self.directory = directory
        with open('chart_template.html', 'r') as myfile:
            self.chart_template = myfile.read()

    def plot(self):
        filepath = os.path.join(self.directory, self.ts.name + ".html")
        empty = True
        for ts in ts_list:
            if not ts.is_empty():
                empty = False
                break

        if empty:
            print("Skipping plot of " + filepath + "; timeseries are empty")
            return

        rows = ""
        first = True
        for item in self.ts.items:
            rows += "[" + str(item.time) + "," + str(item.value) + "]"
            if first:
                rows += ","
            rows += "\n"

        chart = self.chart_template.replace("$rows", rows)
        ensure_dir(self.directory)

        file = open(filepath, 'w')
        file.write(chart)
        file.close()
        print filepath


# a series is effectively a list of key/values. It could be a time series where the key is the time and the value
# is the measured value e.g. cpu usage.
class Series:
    name = None

    def __init__(self, name, ylabel, is_bytes, ts_list=None, items=None, ):
        if ts_list is None:
            ts_list = []

        self.is_bytes = is_bytes
        self.name = name
        self.ylabel = ylabel
        if not items:
            self.items = []
        else:
            self.items = items
        self.attributes = {}

        for source_ts in ts_list:
            if source_ts.is_empty():
                continue

            # add all items in the source_ts, to the result_ts
            for index in range(0, source_ts.length()):
                source_item = source_ts.items[index]
                if self.length() > index:
                    result_item = self.items[index]
                    result_item.value += source_item.value
                else:
                    self.add(source_item.time, source_item.value)

    def add(self, time, value):
        self.items.append(KeyValue(time, value))

    def start_time(self):
        if not self.items:
            return None
        else:
            return self.items[0].time

    def end_time(self):
        if not self.items:
            return None
        else:
            return self.items[len(self.items) - 1].time

    def to_tmp_file(self):
        temp = tempfile.NamedTemporaryFile(delete=False)
        for item in self.items:
            temp.write(str(item.time) + ',' + str(item.value) + '\n')
        temp.close()
        return temp

    def length(self):
        return len(self.items)

    def is_empty(self):
        return self.length() == 0

    def min(self):
        result = None
        for item in self.items:
            if not result or item.value < result:
                result = item.value
        return result

    def max(self):
        result = None
        for item in self.items:
            if not result or item.value > result:
                result = item.value
        return result

# A key/value in a series
class KeyValue:
    time = None
    value = None

    def __init__(self, time, value):
        self.time = time
        self.value = float(value)


# A handle to a series. With a handle you can refer to a series, without needing to pull it into memory. Since we could have
# a lot of measured data, we want to prevent getting it all in memory.
class SeriesHandle:

    def __init__(self, src, name, title, ylabel, load_method, args=None, is_bytes=False):
        if not args:
            args = []

        self.src = src
        self.name = name
        self.title = title
        self.ylabel = ylabel
        self.load_method = load_method
        self.args = args
        self.is_bytes = is_bytes

    def load(self):
        items = self.load_method(*self.args)
        return Series(self.name, self.ylabel, self.is_bytes, items=items)


class Worker:
    name = ""
    directory = ""
    performance_csv = None

    def __init__(self, name, directory):
        self.name = name
        self.directory = directory

        refs = []
        self.ts_references = refs
        refs.append(SeriesHandle("throughput", "throughput_" + name, "Throughput", "Operations/second",
                                 self.__load_throughput))

        refs.append(SeriesHandle("dstat", "memory_used", "Memory Used", "Memory used",
                                 self.__load_dstat, args=[1], is_bytes=True))
        refs.append(SeriesHandle("dstat", "memory_buffered", "Memory Buffered", "Memory Buffered",
                                 self.__load_dstat, args=[2], is_bytes=True))
        refs.append(SeriesHandle("dstat", "memory_cached", "Memory Cached", "Memory Cached",
                                 self.__load_dstat, args=[3], is_bytes=True))
        refs.append(SeriesHandle("dstat", "memory_free", "Memory Free", "Memory Free",
                                 self.__load_dstat, args=[4], is_bytes=True))

        refs.append(SeriesHandle("dstat", "cpu_user", "CPU User", "CPU User %",
                                 self.__load_dstat, args=[5]))
        refs.append(SeriesHandle("dstat", "cpu_system", "CPU System", "CPU System %",
                                 self.__load_dstat, args=[6]))
        refs.append(SeriesHandle("dstat", "cpu_idle", "CPU Idle", "CPU Idle %",
                                 self.__load_dstat, args=[7]))
        refs.append(SeriesHandle("dstat", "cpu_wait", "CPU Wait", "CPU Wait %",
                                 self.__load_dstat, args=[8]))
        refs.append(SeriesHandle("dstat", "cpu_total", "CPU Total", "CPU Total %",
                                 self.__load_dstat_cpu_total_ts))

        refs.append(SeriesHandle("dstat", "cpu_hardware_interrupts", "CPU Hardware Interrupts", "CPU Hardware Interrupts/sec",
                                 self.__load_dstat, args=[9]))
        refs.append(SeriesHandle("dstat", "cpu_software_interrupts", "CPU Software Interrupts", "CPU Software Interrupts/sec",
                                 self.__load_dstat, args=[10]))

        refs.append(SeriesHandle("dstat", "disk_read", "Disk Reads", "Disk Reads/sec",
                                 self.__load_dstat, args=[11], is_bytes=True))
        refs.append(SeriesHandle("dstat", "disk_write", "Disk Writes", "Disk writes/sec",
                                 self.__load_dstat, args=[12], is_bytes=True))

        refs.append(SeriesHandle("dstat", "net_receive", "Net Receive", "Receiving/second",
                                 self.__load_dstat, args=[13], is_bytes=True))
        refs.append(SeriesHandle("dstat", "net_send", "Net Send", "Sending/second",
                                 self.__load_dstat, args=[14], is_bytes=True))

        refs.append(SeriesHandle("dstat", "page_in", "Page in", "todo",
                                 self.__load_dstat, args=[15]))
        refs.append(SeriesHandle("dstat", "page_out", "Page out", "todo",
                                 self.__load_dstat, args=[16]))

        refs.append(SeriesHandle("dstat", "system_interrupts", "System Interrupts", "System Interrupts/sec",
                                 self.__load_dstat, args=[17]))
        refs.append(SeriesHandle("dstat", "system_context_switches", "System Context Switches", "System Context Switches/sec",
                                 self.__load_dstat, args=[18]))

        refs.append(SeriesHandle("dstat", "load_average_1m", "Load Average 1 Minute", "Load",
                                 self.__load_dstat, args=[19]))
        refs.append(SeriesHandle("dstat", "load_average_5m", "Load Average 5 Minutes", "Load",
                                 self.__load_dstat, args=[20]))
        refs.append(SeriesHandle("dstat", "load_average_15m", "Load Average 15 Minute", "Load",
                                 self.__load_dstat, args=[21]))

    # Returns the name of the agent this worker belongs to
    def agent(self):
        index = self.name.index("_", 3)
        return self.name[0:index]

    def is_driver(self):
        return os.path.exists(self.performance_csv)

    def __load_throughput(self):
        performance_csv = os.path.join(self.directory, "performance.csv")

        result = []
        if os.path.exists(performance_csv):
            with open(performance_csv, 'rb') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                # skip the first line
                next(csvreader)
                for row in csvreader:
                    result.append(KeyValue(row[0], row[4]))
        return result

    def __load_dstat(self, column):
        dstat_csv = os.path.join(self.directory, "dstat.csv")

        result = []
        if os.path.exists(dstat_csv):
            with open(dstat_csv, 'rb') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                # we need to skip the first 7 lines
                for x in range(0, 8):
                    next(csvreader)
                for row in csvreader:
                    result.append(KeyValue(row[0], row[column]))
        return result

    # total cpu usage isn't explicitly provided by dstat, so we just sum the user+system
    def __load_dstat_cpu_total_ts(self):
        dstat_csv = os.path.join(self.directory, "dstat.csv")

        result = []
        if os.path.exists(dstat_csv):
            with open(dstat_csv, 'rb') as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                # we need to skip the first 7 lines
                for x in range(0, 8):
                    next(csvreader)
                for row in csvreader:
                    result.append(KeyValue(row[0], float(row[5])+float(row[6])))
        return result


class Benchmark:
    # the directory where the original files can be found
    src_dir = ""
    target_dir = ""
    workers = None
    name = ""

    def __init__(self, src_dir, name):
        self.src_dir = src_dir
        self.name = name

        self.target_dir = os.path.join(output_dir, self.name)
        ensure_dir(self.target_dir)

        # load all workers
        self.workers = []
        for subdir_name in os.listdir(src_dir):
            subdir = os.path.join(src_dir, subdir_name)
            if not os.path.isdir(subdir):
                continue
            if not subdir_name.startswith("C_A"):
                continue
            self.workers.append(Worker(subdir_name, subdir))

        # making sure there are workers; otherwise it is an invalid benchmark
        if len(self.workers) == 0:
            print("Invalid Benchmark "+self.name+" from directory ["+self.src_dir+"]; no workers found")
            exit(1)

        # look for all latency info
        refs = []
        self.ts_references = refs

        refs.append(SeriesHandle("throughput", "throughput", "Throughput", "Operations/sec", self.aggregated_throughput))

        for file_name in os.listdir(self.src_dir):
            if not file_name.endswith(".hgrm"):
                continue

            file_name = os.path.splitext(file_name)[0]
            file_path = os.path.join(self.src_dir, file_name)
            print(file_path)

            name=file_name.split('-')[1]

            refs.append(SeriesHandle("latency", "latency_interval_25_" + name, "Interval 25%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 3]))
            refs.append(SeriesHandle("latency", "latency_interval_50_" + name, "Interval 50%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 4]))
            refs.append(SeriesHandle("latency", "latency_interval_75_" + name, "Interval 75%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 5]))
            refs.append(SeriesHandle("latency", "latency_interval_90_" + name, "Interval 90%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 6]))
            refs.append(SeriesHandle("latency", "latency_interval_99_" + name, "Interval 99%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 7]))
            refs.append(SeriesHandle("latency", "latency_interval_999_" + name, "Interval 99.9%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 8]))
            refs.append(SeriesHandle("latency", "latency_interval_9999_" + name, "Interval 99.99%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 9]))
            refs.append(SeriesHandle("latency", "latency_interval_99999_" + name, "Interval 99.999%", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 10]))

            refs.append(SeriesHandle("latency", "latency_interval_min_" + name, "Interval Min", "Latency (μs)",
                                     self.load_latency_ts,args=[file_path, 11]))
            refs.append(SeriesHandle("latency", "latency_interval_max_" + name, "Interval Max", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 12]))
            refs.append(SeriesHandle("latency", "latency_interval_mean_" + name, "Interval Mean", "Latency (μs)",
                                     self.load_latency_ts,args=[file_path, 13]))
            refs.append(SeriesHandle("latency", "latency_interval_std_deviation_" + name, "Interval Standard Deviation", "Latency (μs)",
                                     self.load_latency_ts, args=[file_path, 14]))

            hgrm_path = os.path.join(src_dir, file_name + ".hgrm")
            refs.append(SeriesHandle("latency-distribution", "latency_distribution_" + name, "Latency distribution", "Latency (μs)",
                                     self.load_latency_distribution_ts, args=[hgrm_path]))

        agents = {}
        for worker in self.workers:
            agent = worker.agent()
            if not agents.get(agent):
                agents[agent] = worker

        for agent, worker in agents.iteritems():
            for ref in worker.ts_references:
                if ref.src == "dstat":
                    refs.append(SeriesHandle("dstat",ref.name+"_"+agent,ref.title, ref.ylabel,self.x, args=[ref], is_bytes=ref.is_bytes))

    def x(self, ref):
        return ref.load().items

    def aggregated_throughput(self):
        list = []
        for worker in self.workers:
            for ref in worker.ts_references:
                if ref.src == "throughput":
                    list.append(ref.load())

        return Series("", "", False, ts_list=list).items

    def load_latency_ts(self, path, column):
        result = []
        with open(path, 'rb') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            # we need to skip the first 7 lines
            for x in range(0, 3):
                next(csvreader)
            for row in csvreader:
                result.append(KeyValue(row[0], row[column]))
        return result

    def load_latency_distribution_ts(self, path):
        result = []

        line_index = 0
        with open(path) as f:
            for line in f:
                line = line.rstrip()
                line_index += 1

                if line_index < 4 or line.startswith("#"):
                    continue

                row = re.split(" +", line)
                if len(row) < 5:
                    continue

                result.append(KeyValue(row[4], row[1]))
        print path
        return result

    def plot_per_worker(self):
        for worker in self.workers:
            output_dir = os.path.join(self.target_dir, worker.name)

            for ts_ref in worker.ts_references:
                if ts_ref.src == "dstat":
                    continue

                ts = ts_ref.load()
                TimeseriesGnuplot(output_dir, ts_ref.title).add(ts).plot()


class Comparison:

    def __init__(self):
        benchmark_dirs = []
        benchmark_names = {}
        last_benchmark = None

        print("Loading benchmarks")

        # collect all benchmark directories and the names for the benchmarks
        for arg in args:
            if arg.startswith("[") and arg.endswith("]"):
                if not last_benchmark:
                    print("Benchmark name "+arg+" must be preceded with a benchmark directory.")
                    exit()
                benchmark_names[last_benchmark] = arg[1:len(arg)-1]
                last_benchmark = None
            else:
                benchmark_dir = arg
                if not os.path.exists(benchmark_dir):
                    print("benchmark directory '" + benchmark_dir + "' does not exist!")
                    exit(1)
                last_benchmark=arg
                benchmark_dirs.append(benchmark_dir)
                name = os.path.basename(os.path.normpath(benchmark_dir))
                benchmark_names[benchmark_dir] = name

        # Make the benchmarks
        self.benchmarks = []
        for benchmark_dir in benchmark_dirs:
            self.benchmarks.append(Benchmark(benchmark_dir, benchmark_names[benchmark_dir]))

    def compare(self):
        plots = {}

        for benchmark in self.benchmarks:
            if len(benchmark.ts_references) == 0:
                print(" benchmark ["+benchmark.name+"] benchmark.dir ["+benchmark.src_dir+"] has no data")
                exit(1)

            for ref in benchmark.ts_references:
                plot = plots.get(ref.name)
                if not plot:
                    if ref.src == "latency-distribution":
                        plot = LatencyDistributionGnuplot(output_dir, ref.title)
                    else:
                        plot = TimeseriesGnuplot(output_dir, ref.title)

                    plots[ref.name] = plot

                plot.add(ref.load(), title=benchmark.name)

        for plot in plots.values():
            plot.plot()

        print("Done writing report [" + output_dir + "]")
        for benchmark in self.benchmarks:
            print(" benchmark ["+benchmark.name+"] benchmark.dir ["+benchmark.src_dir+"]")



comparison = Comparison()
comparison.compare()


