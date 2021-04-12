#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# todo:
# - writing html
# - if no latency info is found; print warning
# - when not a lot of data points, them time issues in gnuplot (use WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS=default)
# - timeseries: avg
# - gnuplot y axis formatting; long numbers are unreadable because not dots or comma's
# - gc aggregated
# - hdr

# backlog
# - google chart option
# - latency per worker
# - option to plot with real time.
# - cpu usage merging needs to be divided by number of agents.
# - option not to make low part of graph shrink
# - option to show real time

import argparse
import csv
import os
import re
import subprocess
import shutil
from collections import Counter
import base64

parser = argparse.ArgumentParser(description='Creating a benchmark report from one or more benchmarks.')
parser.add_argument('benchmarks', metavar='B', nargs='+',
                    help='a benchmark to be used in the comparison')
# parser.add_argument('-r', '--realtime', default='report', help='print the real time of the datapoints.')
parser.add_argument('-o', '--output', nargs=1,
                    help='The output directory for the report. By default a report directory in the working directory is created.')
parser.add_argument('-w', '--warmup', nargs=1, default=[0], type=int,
                    help='The warmup period in seconds. The warmup removes datapoints from the start.')
parser.add_argument('-c', '--cooldown', nargs=1, default=[0], type=int,
                    help='The cooldown period in seconds. The cooldown removes datapoints from the end.')
parser.add_argument('-f', '--full', help='Enable individual worker level diagrams.', action="store_true")
parser.add_argument('--svg', help='SVG instead of PNG graphics.', action="store_true")

args = parser.parse_args()
benchmark_args = args.benchmarks

gc_logs_found = False

simulator_home = os.environ['SIMULATOR_HOME']
os.environ['LC_CTYPE'] = "en_US.UTF-8"

if not args.output:
    report_dir = "report"
else:
    report_dir = args.output[0]
report_dir = os.path.abspath(report_dir)

warmup_seconds = int(args.warmup[0])
cooldown_seconds = int(args.cooldown[0])

print("Report directory '" + report_dir + "'")


# ================ utils ========================

def dump(obj):
    for attr in dir(obj):
        print(("obj.%s = %s" % (attr, getattr(obj, attr))))


def ensure_dir(file_path):
    if not os.path.exists(file_path):
        os.makedirs(file_path)


# Returns the name of the agent this worker belongs to
def agent_for_worker(worker_name):
    if worker_name.startswith("C_"):
        # for compatibility with old benchmarks
        index = worker_name.index("_", 3)
        return worker_name[0:index]
    else:
        index = worker_name.index("_")
        return worker_name[0:index]


# ================ plotting =========================

class Gnuplot:
    image_width = 1280
    image_height = 1024
    image_path = None
    ylabel = None
    is_bytes = None
    is_points = None
    script_file = None

    def __init__(self, directory, title, basefilename=None):
        self.title = title
        self.directory = directory
        self.ts_list = []
        self.titles = {}
        self.basefilename = basefilename

    def _complete(self):
        self.script_file.close()
        from os import system
        system('gnuplot ' + self.script_file.name)

    def _write(self, line):
        self.script_file.write(line + '\n')

    def add(self, ts, title=None):
        self.ts_list.append(ts)
        self.titles[ts] = title
        return self

    # returns a color for the time series. We are using some hard
    # coded colors to make sure the the colors are predictable and
    # very much different. If there are too many time series
    # then we just rely on the default mechanism
    def _color(self, ts):
        if (len(self.ts_list)) > 8:
            return None

        # for list of colors: http://www.ss.scphys.kyoto-u.ac.jp/person/yonezawa/contents/program/gnuplot/colorname_list.html
        if ts == self.ts_list[0]:
            return "red"
        elif ts == self.ts_list[1]:
            return "blue"
        elif ts == self.ts_list[2]:
            return "forest-green"
        elif ts == self.ts_list[3]:
            return "orchid"
        elif ts == self.ts_list[4]:
            return "grey"
        elif ts == self.ts_list[5]:
            return "brown"
        elif ts == self.ts_list[6]:
            return "violet"
        else:
            return "dark-goldenrod"

    def plot(self):
        empty = True
        for ts in self.ts_list:
            if not ts.is_empty():
                empty = False
                break

        if empty:
            print("Skipping plot of " + self.title + "; empty time series")
            return

        ts_first = self.ts_list[0]
        self.ylabel = ts_first.ylabel

        if args.svg:
            ext = "svg"
        else:
            ext = "png"

        if self.basefilename:
            self.image_path = os.path.join(self.directory, self.basefilename + "." + ext)
            script_path = os.path.join(self.directory, self.basefilename + ".plt")
        else:
            self.image_path = os.path.join(self.directory, ts_first.name + "." + ext)
            script_path = os.path.join(self.directory, ts_first.name + ".plt")

        self.is_bytes = ts_first.is_bytes
        self.is_points = ts_first.is_points
        ensure_dir(self.directory)
        self.script_file = open(script_path, "w")
        self._plot()

        print(self.image_path)

    def _plot(self):
        raise NotImplementedError("Please Implement this method")


class TimeseriesGnuplot(Gnuplot):
    def __init__(self, directory, title, basefilename=None):
        Gnuplot.__init__(self, directory, title, basefilename)

    def _plot(self):
        # self._write("unset autoscale y")
        self._write("set title '" + self.title + "' noenhanced")
        self._write("set style data lines")
        self._write('set datafile separator ","')

        if args.svg:
            self._write("set term svg enhanced mouse size " + str(self.image_width) + "," + str(self.image_height))
        else:
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

        # else:
        #    self._write("set format y '%.0f'")

        self._write("set output '" + self.image_path + "'")
        self._write("plot \\")

        for ts in self.ts_list:
            ts_file = ts.to_data_file()

            if len(self.ts_list) > 1:
                title = self.titles[ts]
                if not title:
                    title = ts.name
                title_str = "title \"" + title + "\" noenhanced"
            else:
                title_str = "title \"\""

            color = self._color(ts)

            if self.is_points:
                lc = ""
                if color:
                    lc = "lc rgb \"" + color + "\""
                self._write(
                    "   \'" + ts_file.name + "\' using (t0(timecolumn(1))):2 " + title_str + " with points " + lc + ", \\")
            else:
                lt = ""
                if color:
                    lt = "lt rgb \"" + color + "\""
                self._write("   \'" + ts_file.name + "\' using (t0(timecolumn(1))):2 " + title_str + " " + lt + ", \\")
        self._complete()


class LatencyDistributionGnuplot(Gnuplot):
    def __init__(self, directory, title, basefilename=None):
        Gnuplot.__init__(self, directory, title, basefilename)

    def _plot(self):
        self._write("set datafile separator \",\"")
        self._write("set title '" + self.title + "' noenhanced")

        if args.svg:
            self._write("set term svg enhanced mouse size " + str(self.image_width) + "," + str(self.image_height))
        else:
            self._write("set terminal png size " + str(self.image_width) + "," + str(self.image_height))

        self._write("set grid")
        self._write("unset xtics")
        self._write("set ylabel 'Latency (μs)'")
        self._write("set logscale x")
#        if slef.logscaley:
#            self._write("set logscale y")
        self._write('set key top left')
        self._write("set style line 1 lt 1 lw 3 pt 3 linecolor rgb \"red\"")
        self._write("set output '" + self.image_path + "'")

        self._write("plot '" + simulator_home + "/bin/xlabels.csv' notitle with labels center offset 0, 1.5 point,\\")
        for ts in self.ts_list:
            ts_file = ts.to_data_file()

            if len(self.ts_list) > 1:
                title = self.titles[ts]
                if not title:
                    title = ts.name
                title_str = "title \"" + title + "\" noenhanced"
            else:
                title_str = "title \"\""

            color = self._color(ts)
            lt = ""
            if color:
                lt = "lt rgb \"" + color + "\""

            self._write("   \"" + ts_file.name + "\" using 1:2 " + title_str + " " + lt + " with lines, \\")

        self._complete()


class GoogleCharts:
    def __init__(self, ts, directory, title):
        self.title = title
        self.ts = ts
        self.directory = directory
        with open('chart_template.html', 'r') as f:
            self.chart_template = f.read()

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

        with open(filepath, 'w') as f:
            f.write(chart)
        print(filepath)


seriesCounter = Counter()

# a series is a list of key/values. It could be a time series where the key is the time and the value
# is the measured value e.g. cpu usage.
class Series:
    name = None

    def __init__(self, name, ylabel, is_bytes, is_points, ts_list=None, items=None, ):
        if ts_list is None:
            ts_list = []

        self.is_points = is_points
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

    def to_data_file(self):
        data_dir = os.path.join(report_dir, "data")
        ensure_dir(data_dir)

        count = seriesCounter.get(self.name, 1)
        seriesCounter[self.name] = count + 1

        file_name = os.path.join(data_dir, self.name + "_" + str(count) + ".data")

        file = open(file_name, "w")
        for item in self.items:
            file.write(str(item.time) + ',' + str(item.value) + '\n')
        file.close()
        return file

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

    # Removes all items from this time series that are not in between the start/end-time.
    def trim(self, start_time, end_time):
        new_items = []
        for item in self.items:
            if start_time is not None and float(item.time) < float(start_time):
                continue
            if end_time is not None and float(item.time) > float(end_time):
                continue
            new_items.append(item)
        self.items = new_items


# A key/value in a series
class KeyValue:
    time = None
    value = None

    def __init__(self, time, value):
        self.time = time
        self.value = float(value)


# A handle to a series. With a handle you can refer to a series, without needing to pull it
# into memory. Since we could have a lot of measured data, we want to prevent getting it all in memory.
class SeriesHandle:
    def __init__(self, src, name, title, ylabel, load_method,
                 args=None, is_bytes=False, is_points=False, start_time=None, end_time=None):
        if not args:
            args = []

        self.src = src
        self.name = name
        self.title = title
        self.ylabel = ylabel
        self.load_method = load_method
        self.args = args
        self.is_bytes = is_bytes
        self.is_points = is_points
        self.start_time = start_time
        self.end_time = end_time

    def period(self, period):
        self.start_time = period.start_time
        self.end_time = period.end_time

    def load(self):
        items = self.load_method(*self.args)
        series = Series(self.name, self.ylabel, self.is_bytes, self.is_points, items=items)
        series.trim(self.start_time, self.end_time)
        return series


class HdrAnalyzer:
    def __init__(self, directory):
        self.directory = directory

    def analyze(self):
        handles = []
        for file_name in os.listdir(self.directory):
            if not file_name.endswith(".hgrm"):
                continue

            file_name = os.path.splitext(file_name)[0]
            file_path = os.path.join(self.directory, file_name)
            name = file_name.split('-')[1]

            handles.append(
                SeriesHandle("latency", "latency_interval_25_" + name, "Interval 25%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 3]))
            handles.append(
                SeriesHandle("latency", "latency_interval_50_" + name, "Interval 50%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 4]))
            handles.append(
                SeriesHandle("latency", "latency_interval_75_" + name, "Interval 75%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 5]))
            handles.append(
                SeriesHandle("latency", "latency_interval_90_" + name, "Interval 90%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 6]))
            handles.append(
                SeriesHandle("latency", "latency_interval_99_" + name, "Interval 99%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 7]))
            handles.append(
                SeriesHandle("latency", "latency_interval_999_" + name, "Interval 99.9%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 8]))
            handles.append(
                SeriesHandle("latency", "latency_interval_9999_" + name, "Interval 99.99%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 9]))
            handles.append(
                SeriesHandle("latency", "latency_interval_99999_" + name, "Interval 99.999%", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 10]))

            handles.append(
                SeriesHandle("latency", "latency_interval_min_" + name, "Interval Min", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 11]))
            handles.append(
                SeriesHandle("latency", "latency_interval_max_" + name, "Interval Max", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 12]))
            handles.append(
                SeriesHandle("latency", "latency_interval_mean_" + name, "Interval Mean", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 13]))
            handles.append(
                SeriesHandle("latency", "latency_interval_std_deviation_" + name, "Interval Standard Deviation", "Latency (μs)",
                             self._load_latency_ts, args=[file_path, 14]))

            hgrm_path = os.path.join(self.directory, file_name + ".hgrm")
            handles.append(
                SeriesHandle("latency-distribution", "latency_distribution_" + name, "Latency distribution", "Latency (μs)",
                             self._load_latency_distribution_ts, args=[hgrm_path]))

        return handles

    def _load_latency_ts(self, path, column):
        result = []
        with open(path) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            # we need to skip the first 7 lines
            for x in range(0, 3):
                next(csvreader)
            for row in csvreader:
                result.append(KeyValue(row[0], row[column]))
        return result

    def _load_latency_distribution_ts(self, path):
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
        return result


class GcAnalyzer:

    def __init__(self, worker_dir, period):
        self.worker_dir = worker_dir
        self.period = period

    def analyze(self):
        self.__make_gc_csv()

        if os.path.exists(os.path.join(self.worker_dir, "gc.csv")):
            global gc_logs_found
            gc_logs_found = True

        handles = []

        handles.append(
            SeriesHandle("gc", "pause_time", "Pause time", "seconds",
                         self.__load_gc, args=[1, True], is_points=True))
        handles.append(
            SeriesHandle("gc", "young_size_before_gc", "Young size before gc", "Size",
                         self.__load_gc, args=[5, True], is_bytes=True))

        handles.append(
            SeriesHandle("gc", "young_size_after_gc", "Young size after gc", "Size",
                         self.__load_gc, args=[6, True], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "young_size_max", "Young size max", "Size",
                         self.__load_gc, args=[7, True], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "young_collected", "Young collected", "Collected",
                         self.__load_gc, args=[8, True], is_bytes=True, is_points=True))
        handles.append(
            SeriesHandle("gc", "young_collected_rate", "Young collection rate", "Collected/sec",
                         self.__load_gc, args=[9, True], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "young_allocated", "Young allocated", "Allocation",
                         self.__load_gc, args=[10, True], is_bytes=True, is_points=True))
        handles.append(
            SeriesHandle("gc", "allocation_rate", "Allocation rate", "Allocated/sec",
                         self.__load_gc, args=[11, True], is_bytes=True))

        handles.append(
            SeriesHandle("gc", "heap_size_before_gc", "Heap size before gc", "Size",
                         self.__load_gc, args=[12, False], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "heap_size_after_gc", "Heap size after gc", "Size",
                         self.__load_gc, args=[13, False], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "heap_size_max", "Heap size max", "Size",
                         self.__load_gc, args=[14, False], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "heap_collected", "Heap collected", "Size",
                         self.__load_gc, args=[15, False], is_bytes=True, is_points=True))
        handles.append(
            SeriesHandle("gc", "heap_collected_rate", "Heap collected rate", "Collected/sec",
                         self.__load_gc, args=[16, False], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "promotion", "Promoted", "Size",
                         self.__load_gc, args=[17, False], is_bytes=True, is_points=True))
        handles.append(
            SeriesHandle("gc", "promotion_rate", "Promotion rate", "Promoted/sec",
                         self.__load_gc, args=[18, True], is_bytes=True))

        handles.append(
            SeriesHandle("gc", "old_size_before_gc", "Tenured size before gc", "Size",
                         self.__load_gc, args=[19, True], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "old_size_after_gc", "Tenured size after gc", "Size",
                         self.__load_gc, args=[20, True], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "old_total", "Tenured size total", "Size",
                         self.__load_gc, args=[21, True], is_bytes=True))

        handles.append(
            SeriesHandle("gc", "meta_size_before_gc", "Meta/Perm size before gc", "Size",
                         self.__load_gc, args=[22, True], is_bytes=True))

        handles.append(
            SeriesHandle("gc", "meta_size_after_gc", "Meta/Perm size after gc", "Size",
                         self.__load_gc, args=[23, True], is_bytes=True))
        handles.append(
            SeriesHandle("gc", "meta_total", "Meta/Perm size total", "Size",
                         self.__load_gc, args=[24, True], is_bytes=True))

        # set the period
        for handle in handles:
            handle.period(self.period)

        return handles

    # Extracts a gc.csv file from the gc.log file
    def __make_gc_csv(self):
        gc_csv = os.path.join(self.worker_dir, "gc.csv")
        gc_log = os.path.join(self.worker_dir, "gc.log")

        # if there is no gc.log file, we are done
        if not os.path.exists(gc_log):
            return

        cmd = "java -jar " + simulator_home + "/lib/gcviewer-1.35-SNAPSHOT.jar " + gc_log + " " + gc_csv + " -t CSV_FULL"
        print(cmd)
        with open(os.devnull, 'w') as devnull:
            subprocess.check_call(cmd.split(), stdout=devnull, stderr=devnull)

    # Loads the data from the gc.csv file
    def __load_gc(self, column, filter_minus_one):
        gc_csv = os.path.join(self.worker_dir, "gc.csv")

        result = []
        if os.path.exists(gc_csv):
            with open(gc_csv) as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                # we need to skip the first line
                next(csvreader)

                for row in csvreader:
                    key = row[0]
                    value = row[column]
                    if value != "-1" or not filter_minus_one:
                        result.append(KeyValue(key, value))
        return result


class DstatAnalyzer:
    def __init__(self, directory, period):
        self.directory = directory
        self.period = period

    def analyze(self):
        handles = []

        # Load the dstat data
        for file_name in os.listdir(self.directory):
            if not file_name.endswith("_dstat.csv"):
                continue

            agent_name = agent_for_worker(file_name)
            period = self.period
            dstat_file = os.path.join(self.directory, file_name)

            handles.append(
                SeriesHandle("dstat", "memory_used_" + agent_name, "Memory Used", "Memory used",
                             self.__load_dstat, args=[1, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "memory_buffered_" + agent_name, "Memory Buffered", "Memory Buffered",
                             self.__load_dstat, args=[2, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "memory_cached_" + agent_name, "Memory Cached", "Memory Cached",
                             self.__load_dstat, args=[3, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "memory_free_" + agent_name, "Memory Free", "Memory Free",
                             self.__load_dstat, args=[4, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))

            handles.append(
                SeriesHandle("dstat", "cpu_user_" + agent_name, "CPU User", "CPU User %",
                             self.__load_dstat, args=[5, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "cpu_system_" + agent_name, "CPU System", "CPU System %",
                             self.__load_dstat, args=[6, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "cpu_idle_" + agent_name, "CPU Idle", "CPU Idle %",
                             self.__load_dstat, args=[7, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "cpu_wait_" + agent_name, "CPU Wait", "CPU Wait %",
                             self.__load_dstat, args=[8, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "cpu_total_" + agent_name, "CPU Total", "CPU Total %",
                             self.__load_dstat_cpu_total_ts, args=[dstat_file],
                             start_time=period.start_time, end_time=period.end_time))

            handles.append(
                SeriesHandle("dstat", "cpu_hardware_interrupts_" + agent_name, "CPU Hardware Interrupts",
                             "CPU Hardware Interrupts/sec",
                             self.__load_dstat, args=[9, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "cpu_software_interrupts_" + agent_name, "CPU Software Interrupts",
                             "CPU Software Interrupts/sec",
                             self.__load_dstat, args=[10, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))

            handles.append(
                SeriesHandle("dstat", "disk_read_" + agent_name, "Disk Reads", "Disk Reads/sec",
                             self.__load_dstat, args=[11, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "disk_write_" + agent_name, "Disk Writes", "Disk writes/sec",
                             self.__load_dstat, args=[12, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))

            handles.append(
                SeriesHandle("dstat", "net_receive_" + agent_name, "Net Receive", "Receiving/sec",
                             self.__load_dstat, args=[13, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "net_send_" + agent_name, "Net Send", "Sending/sec",
                             self.__load_dstat, args=[14, dstat_file], is_bytes=True,
                             start_time=period.start_time, end_time=period.end_time))

            handles.append(
                SeriesHandle("dstat", "page_in_" + agent_name, "Page in", "Pages/sec",
                             self.__load_dstat, args=[15, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "page_out_" + agent_name, "Page out", "Pages/sec",
                             self.__load_dstat, args=[16, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))

            handles.append(
                SeriesHandle("dstat", "system_interrupts_" + agent_name, "System Interrupts", "System Interrupts/sec",
                             self.__load_dstat, args=[17, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "system_context_switches_" + agent_name, "System Context Switches",
                             "System Context Switches/sec",
                             self.__load_dstat, args=[18, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))

            handles.append(
                SeriesHandle("dstat", "load_average_1m_" + agent_name, "Load Average 1 Minute", "Load",
                             self.__load_dstat, args=[19, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "load_average_5m_" + agent_name, "Load Average 5 Minutes", "Load",
                             self.__load_dstat, args=[20, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
            handles.append(
                SeriesHandle("dstat", "load_average_15m_" + agent_name, "Load Average 15 Minute", "Load",
                             self.__load_dstat, args=[21, dstat_file],
                             start_time=period.start_time, end_time=period.end_time))
        return handles

    def __load_dstat(self, column, dstat_csv):
        result = []
        if os.path.exists(dstat_csv):
            with open(dstat_csv) as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                # we need to skip the first 7 lines
                for x in range(0, 8):
                    next(csvreader)
                for row in csvreader:
                    if column < len(row):  # protection if column doesn't exist
                        result.append(KeyValue(row[0], row[column]))
        return result

    # total cpu usage isn't explicitly provided by dstat, so we just sum the user+system
    def __load_dstat_cpu_total_ts(self, dstat_csv):
        result = []
        if os.path.exists(dstat_csv):
            with open(dstat_csv) as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                # we need to skip the first 7 lines
                for x in range(0, 8):
                    next(csvreader)
                for row in csvreader:
                    if len(row) > 6:  # protection if column doesn't exist
                        result.append(KeyValue(row[0], float(row[5]) + float(row[6])))
        return result


# Analyzes the perform.csv for a worker.
class ThroughputAnalyzer:
    def __init__(self, worker_dir, worker_name, period):
        self.worker_dir = worker_dir
        self.worker_name = worker_name
        self.period = period

    def analyze(self):
        handles = []
        handles.append(
            SeriesHandle("throughput", "throughput_" + self.worker_name, "Throughput", "Operations/sec",
                         self.__load_throughput,
                         start_time=self.period.start_time,
                         end_time=self.period.end_time))
        return handles

    def __load_throughput(self):
        performance_csv = os.path.join(self.worker_dir, "performance.csv")
        result = []
        if os.path.exists(performance_csv):
            with open(performance_csv) as csvfile:
                csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                # skip the first line
                next(csvreader)
                for row in csvreader:
                    result.append(KeyValue(row[0], row[4]))
        return result


class Worker:
    name = ""
    worker_dir = ""

    def __init__(self, worker_dir, period):
        self.worker_dir = worker_dir
        self.name = os.path.basename(worker_dir)
        self.period = period
        self.handles = []
        self.handles.extend(ThroughputAnalyzer(self.worker_dir, self.name, period).analyze())
        self.handles.extend(GcAnalyzer(self.worker_dir, period).analyze())
        self.handles.extend(HdrAnalyzer(self.worker_dir).analyze())


class Period:
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time

    def start_millis(self):
        return int(round(float(self.start_time)*1000))

    def end_millis(self):
        return int(round(float(self.end_time)*1000))

class Benchmark:
    # the directory where the original files can be found
    src_dir = ""
    workers = None
    name = ""
    period = None

    def __init__(self, src_dir, name, id):
        self.src_dir = src_dir
        self.name = name
        self.handles = []
        self.id=id

    def load_workers(self):
        # load all workers
        self.workers = []
        for subdir_name in os.listdir(self.src_dir):
            subdir = os.path.join(self.src_dir, subdir_name)
            if not os.path.isdir(subdir):
                continue
            if not subdir_name.startswith("A"):
                continue
            agent_name = agent_for_worker(subdir_name)
            self.workers.append(Worker(subdir, self.period))

        # making sure there are workers; otherwise it is an invalid benchmark
        if len(self.workers) == 0:
            print("Invalid Benchmark " + self.name + " from directory [" + self.src_dir + "]; no workers found")
            exit(1)

        self.handles.append(
            SeriesHandle("throughput", "throughput", "Throughput", "Operations/sec", self.aggregated_throughput))
        self.handles.extend(DstatAnalyzer(report_dir+"/tmp/"+str(self.id), self.period).analyze())
        self.handles.extend(HdrAnalyzer(report_dir+"/tmp/"+str(self.id)).analyze())

        agents = {}
        for worker in self.workers:
            agent = agent_for_worker(worker.name)
            if not agents.get(agent):
                agents[agent] = worker

    def lookup_period(self):
        #print("---------------------------------------")

        for worker_name in os.listdir(self.src_dir):
            if not worker_name.startswith("A"):
                continue
            worker_dir = os.path.join(self.src_dir, worker_name)
            if not os.path.isdir(worker_dir):
                continue

            agent_name = agent_for_worker(worker_name)
            performance_csv_file = os.path.join(worker_dir, "performance.csv")
            if not os.path.isfile(performance_csv_file):
                continue

            print(performance_csv_file)
            with open(performance_csv_file) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',', quotechar='|')
                # skip first line
                next(csv_reader)
                first_time = float(next(csv_reader)[0])
                start_time = str(first_time + warmup_seconds)
                end_time = str(first_time - cooldown_seconds)

                for row in csv_reader:
                    v = float(row[0])
                    end_time = str(v - cooldown_seconds)

                #print("agent:"+agent_name)
                #print(str(start_time))
                #print(str(end_time))
                self.period = Period(start_time, end_time)

        #print("---------------------------------------")

    def init_files(self):
        cmd = simulator_home + "/conf/init_report_files.sh " + self.src_dir+ " " + report_dir + " " + str(self.id) + " "+ str(self.period.start_millis()) + " " + str(self.period.end_millis())
        print(cmd)
        out = subprocess.check_output(cmd.split())
        #print(out)

# todo: better name
    def x(self, handle):
        return handle.load().items

    def aggregated_throughput(self):
        ts_list = []
        for worker in self.workers:
            for handle in worker.handles:
                if handle.src == "throughput":
                    ts_list.append(handle.load())
        return Series("", "", False, False, ts_list=ts_list).items

class Comparison:

    def __init__(self):
        benchmark_dirs = []
        benchmark_names = {}
        last_benchmark = None

        print("Loading benchmarks")

        # collect all benchmark directories and the names for the benchmarks
        for benchmark_arg in benchmark_args:
            if benchmark_arg.startswith("[") and benchmark_arg.endswith("]"):
                if not last_benchmark:
                    print("Benchmark name " + benchmark_arg + " must be preceded with a benchmark directory.")
                    exit()
                benchmark_names[last_benchmark] = benchmark_arg[1:len(benchmark_arg) - 1]
                last_benchmark = None
            else:
                benchmark_dir = benchmark_arg
                if not os.path.exists(benchmark_dir):
                    print("benchmark directory '" + benchmark_dir + "' does not exist!")
                    exit(1)
                last_benchmark = benchmark_arg
                benchmark_dirs.append(benchmark_dir)
                name = os.path.basename(os.path.normpath(benchmark_dir))
                benchmark_names[benchmark_dir] = name

        # Make the benchmarks
        benchmark_id=0
        self.benchmarks = []
        for benchmark_dir in benchmark_dirs:
            benchmark_id += 1
            benchmark = Benchmark(benchmark_dir, benchmark_names[benchmark_dir], benchmark_id)
            benchmark.lookup_period()
            benchmark.init_files()
            benchmark.load_workers()
            self.benchmarks.append(benchmark)

    def output_dir(self, name):
        output_dir = os.path.join(report_dir, name)
        ensure_dir(output_dir)
        return output_dir

    # makes the actual comparison report.
    def make(self):
        plots = {}

        # plot benchmark/machine level metrics
        for benchmark in self.benchmarks:
            if len(benchmark.handles) == 0:
                print(" benchmark [" + benchmark.name + "] benchmark.dir [" + benchmark.src_dir + "] has no data")
                exit(1)

            for handle in benchmark.handles:
                plot = plots.get(handle.name)
                if not plot:
                    if handle.src == "latency-distribution":
                        plot = LatencyDistributionGnuplot(self.output_dir("latency"), handle.title)
                    else:
                        plot = TimeseriesGnuplot(self.output_dir(handle.src), handle.title)

                    plots[handle.name] = plot

                plot.add(handle.load(), title=benchmark.name)

        # plot worker level metrics
        if args.full:
            for benchmark in self.benchmarks:
                for worker in benchmark.workers:
                    for handle in worker.handles:
                        if handle.src == "throughput":
                            plot = plots.get("throughput_per_worker")
                            if not plot:
                                plot = TimeseriesGnuplot(self.output_dir(handle.src),
                                                         "Throughput per worker",
                                                         basefilename="throughput_per_worker")
                                plots["throughput_per_worker"] = plot

                            if len(self.benchmarks) > 1:
                                plot.add(handle.load(), benchmark.name + "_" + worker.name)
                            else:
                                plot.add(handle.load(), worker.name)
                        else:
                            name = handle.name + "_" + worker.name
                            plot = plots.get(name)
                            title = worker.name + " " + handle.title
                            if not plot:
                                if handle.src == "latency-distribution":
                                    plot = LatencyDistributionGnuplot(self.output_dir("latency"), title, basefilename=name)
                                else:
                                    plot = TimeseriesGnuplot(self.output_dir(handle.src), title, basefilename=name)
                                plots[name] = plot

                            plot.add(handle.load(), benchmark.name)

        for plot in list(plots.values()):
            plot.plot()
            htmlReport.addImage(plot)

        print("Done writing report [" + report_dir + "]")
        for benchmark in self.benchmarks:
            print(" benchmark [" + benchmark.name + "] benchmark.dir [" + benchmark.src_dir + "]")

class HTMLReport:

    def __init__(self):
        self.report = ""
        self.images = ""
        self.metrics = []
        print("Initialising HTML Report Generation.")

    def addImage(self, plot):
        metric_name = plot.image_path.split('/')[-2]

        encoded_image = ""
        with open(plot.image_path, "rb") as image_file:
           encoded_image = str(base64.b64encode(image_file.read()), encoding='utf-8')

        encoded_image = "data:image/png;base64,%s" % encoded_image

        self.images = self.images + '<div class="image-container ' + metric_name + '">'
        self.images = self.images + '<img src="' + encoded_image + '" onclick="toggleZoom(this);" />'
        self.images = self.images + '<p class="image-text">' + plot.title + "</p>"
        self.images = self.images + '</div>'

    def getCSVContents(self):
        contents = []
        with open(os.path.join(report_dir + "/report.csv")) as csvfile:
            line = csvfile.readline()
            while line != '':
                contents.append(line)
                line = csvfile.readline()

        return contents

    def generate(self):
        csvContents = self.getCSVContents()

        self.report = self.report + '<!DOCTYPE html>'
        self.report = self.report + '<html>'
        self.report = self.report + '<head>'
        self.report = self.report + '<style>'
        self.report = self.report + 'body { background-color: #cdcdcd; text-align: center; } img { width: 40vw; } .images-block { display: block; } h1,h2,h3,h4,h5,h6 { width: 100vw; }'
        self.report = self.report + '.tabs { display: flex; border-bottom: 1px solid black; margin-bottom: 3vh; } .tab { flex: 33.33%; } .tab:hover, .active-tab { background-color: #dedede; }'
        self.report = self.report + 'img:hover { cursor: zoom-in; } .zoomin { zoom: 2; -moz-transform: scale(2); } .zoomout { zoom: normal; -moz-transform: scale(1); }'
        self.report = self.report + 'tr,td { border: 1px solid black; } td { padding: 3px; }'
        self.report = self.report + '</style>'
        self.report = self.report + '<body>'
        self.report = self.report + '<h1>Benchmark Report</h1>'
        self.report = self.report + '<div class="tabs">'
        self.report = self.report + '<div class="tab" id="csv" style="border-right: 1px solid black;"><p>Summary</p></div>'
        self.report = self.report + '<div class="tab" id="throughput" style="border-right: 1px solid black;"><p>Throughput</p></div>'
        self.report = self.report + '<div class="tab" id="latency" style="border-right: 1px solid black;"><p>Latency</p></div>'
        self.report = self.report + '<div class="tab" id="dstat"><p>dstat</p></div>'
        self.report = self.report + '</div>'
        self.report = self.report + '<div class="images-block">' + self.images + '</div>'
        self.report = self.report + '<table><tbody>'
        for i in range(len(csvContents[0].split(','))):
            self.report = self.report + '<tr>'
            for j in range(len(csvContents)):
                self.report = self.report + '<td>' + csvContents[j].split(',')[i].replace('"', '') + '</td>'
            self.report = self.report + '</tr>'
        self.report = self.report + '</table></tbody>'
        self.report = self.report + '<script>'
        self.report = self.report + "var activeTab = 'throughput'; var throughputdom = document.getElementById('throughput'); var latencydom = document.getElementById('latency');  var dstatdom = document.getElementById('dstat'); var csvdom = document.getElementById('csv'); var imageContainer = document.getElementsByClassName('image-container'); var tabledom = document.getElementsByTagName('table')[0]; "
        self.report = self.report + "function addClass(classname, element){ while(element.classList.contains(classname)) { element.classList.remove(classname); } element.classList.add(classname); } "
        self.report = self.report + "function removeClass(classname, element){ while(element.classList.contains(classname)) { element.classList.remove(classname); } } "
        self.report = self.report + "function filter() { tabledom.style.display = 'none'; for(let item of imageContainer){ if(item.classList.contains(activeTab)) item.style.display='block'; else item.style.display = 'none';} } "
        self.report = self.report + "function showcsv() { for(let item of imageContainer){ item.style.display = 'none'; } tabledom.style.display = 'inline'; }"
        self.report = self.report + "throughputdom.addEventListener('click', function(e){ e.preventDefault(); addClass('active-tab', throughputdom); removeClass('active-tab', latencydom); removeClass('active-tab', dstatdom); removeClass('active-tab', csvdom); activeTab = 'throughput'; filter(); }); "
        self.report = self.report + "latencydom.addEventListener('click', function(e){ e.preventDefault(); addClass('active-tab', latencydom); removeClass('active-tab', throughputdom); removeClass('active-tab', dstatdom); removeClass('active-tab', csvdom); activeTab = 'latency'; filter(); }); "
        self.report = self.report + "dstatdom.addEventListener('click', function(e){ e.preventDefault(); addClass('active-tab', dstatdom); removeClass('active-tab', throughputdom); removeClass('active-tab', latencydom); removeClass('active-tab', csvdom); activeTab = 'dstat'; filter(); }); "
        self.report = self.report + "csvdom.addEventListener('click', function(e){ e.preventDefault(); addClass('active-tab', csvdom); removeClass('active-tab', throughputdom); removeClass('active-tab', latencydom); removeClass('active-tab', dstatdom); activeTab = 'csv'; showcsv(); }); "
        self.report = self.report + "csvdom.click(); "
        self.report = self.report + "function toggleZoom(element){ if(element){ if(element.classList.contains('zoomin')) { removeClass('zoomin', element); addClass('zoomout', element); } else { removeClass('zoomout', element); addClass('zoomin', element); } } } "
        self.report = self.report + '</script>'
        self.report = self.report + '</body>'
        self.report = self.report + '</head>'
        self.report = self.report + '</html>'

        file_name = os.path.join(report_dir + "/report.html")
        with open(file_name, 'w') as f:
            f.write(self.report)

        # print a clickable link to report file
        print("HTML report generated at: "+"\u001b]8;;file://"
              + file_name + "\u001b\\" + file_name + "\u001b]8;;\u001b\\")

if os.path.isdir('report'):
    shutil.rmtree('report')

htmlReport = HTMLReport()
comparison = Comparison()
comparison.make()
htmlReport.generate()

if not args.full and gc_logs_found:
    print("gc.log files have been found. Run with -f option to get these plotted.")
