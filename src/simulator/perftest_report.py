#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import glob
import shutil
import subprocess
import base64

from simulator.log import info, error
from simulator.perftest_report_dstat import report_dstat, analyze_dstat
from simulator.perftest_report_hdr import report_hdr, prepare_hdr, analyze_latency_history
from simulator.perftest_report_operations import report_operations, prepare_operation, analyze_operations
from simulator.util import mkdir, simulator_home
from simulator.perftest_report_shared import *


# Returns the name of the agent this worker belongs to
def agent_for_worker(worker_name):
    if worker_name.startswith("C_"):
        # for compatibility with old benchmarks
        index = worker_name.index("_", 3)
        return worker_name[0:index]
    else:
        index = worker_name.index("_")
        return worker_name[0:index]


def prepare(config: ReportConfig):
    if os.path.exists(config.report_dir):
        shutil.rmtree(config.report_dir)
    mkdir(config.report_dir)
    prepare_operation(config)
    prepare_hdr(config)


def analyze(config: ReportConfig):
    df = None

    for run_label, run_dir in config.runs.items():
        if len(config.runs) == 1:
            df = analyze_run(config.report_dir, run_dir, run_label)
        else:
            tmp_df = analyze_run(config.report_dir, run_dir, run_label)
            df = merge_dataframes(df, shift_to_epoch(tmp_df))
    return df


def analyze_run(report_dir, run_dir, run_label=None):
    print(f"Analyzing run_path:{run_dir}")

    if run_label is None:
        run_label = os.path.basename(run_dir)

    attributes = {"run_label": run_label}

    result = None

    df_operations = analyze_operations(run_dir, attributes)
    result = merge_dataframes(result, df_operations)

    df_latency_history = analyze_latency_history(report_dir, run_dir, attributes)
    result = merge_dataframes(result, df_latency_history)

    df_dstat = analyze_dstat(run_dir, attributes)
    result = merge_dataframes(result, df_dstat)

    print(f"Analyzing run_path:{run_dir}: Done")

    return result


def report(config: ReportConfig, df: pd.DataFrame):
    if df is None:
        return

    path_csv = f"{config.report_dir}/data.csv"
    print(f"path csv: {path_csv}")
    df.to_csv(path_csv)

    # for column_name in df.columns:
    #     print(column_name)

    report_operations(config, df)
    report_hdr(config, df)
    report_dstat(config, df)


class Period:
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time

    def start_millis(self):
        return int(round(float(self.start_time) * 1000))

    def end_millis(self):
        return int(round(float(self.end_time) * 1000))


class Benchmark:
    # the directory where the original files can be found
    src_dir = ""
    workers = None
    name = ""
    period = None

    def __init__(self, src_dir, name, id, config: ReportConfig):
        self.src_dir = src_dir
        self.name = name
        self.handles = []
        self.id = id
        self.config = config

    def lookup_period(self):
        for worker_name in os.listdir(self.src_dir):
            if not worker_name.startswith("A"):
                continue
            worker_dir = os.path.join(self.src_dir, worker_name)
            if not os.path.isdir(worker_dir):
                continue

            agent_name = agent_for_worker(worker_name)
            operations_csv_file = os.path.join(worker_dir, "operations.csv")
            if not os.path.isfile(operations_csv_file):
                continue

            with open(operations_csv_file) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',', quotechar='|')
                # skip first line
                next(csv_reader)
                first_time = float(next(csv_reader)[0])
                start_time = str(first_time + self.config.warmup_seconds)
                end_time = str(first_time - self.config.cooldown_seconds)

                for row in csv_reader:
                    v = float(row[0])
                    end_time = str(v - self.config.cooldown_seconds)

                if self.period is None:
                    # first iteration where the period is not set yet
                    self.period = Period(start_time, end_time)
                else:
                    # We need to pick the earliest time from all series for the start.
                    # and the latest for the end.
                    # That way the series don't get trimmed because of milliseconds (minimal worker reporting
                    # interval is 1 second, see WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS property) causing
                    # misalignment of the series by one data point in the chart resulting in ugly vertical drop
                    # at the end of the throughput charts
                    self.period = Period(min(self.period.start_time, start_time), max(self.period.end_time, end_time))

    # todo: better name
    def x(self, handle):
        return handle.load().items


class Comparison:

    def __init__(self, config: ReportConfig):
        benchmark_dirs = []
        benchmark_names = {}
        last_benchmark = None

        info("Loading benchmarks")

        # collect all benchmark directories and the names for the benchmarks
        for benchmark_arg in benchmark_args:
            if benchmark_arg.startswith("[") and benchmark_arg.endswith("]"):
                if not last_benchmark:
                    info("Benchmark name " + benchmark_arg + " must be preceded with a benchmark directory.")
                    exit()
                benchmark_names[last_benchmark] = benchmark_arg[1:len(benchmark_arg) - 1]
                last_benchmark = None
            elif compare_last:
                benchmark_root = benchmark_arg
                subdirectories = sorted(filter(os.path.isdir, glob.glob(benchmark_root + "/*")))

                benchmark_dir = subdirectories[-1]
                if not os.path.exists(benchmark_dir):
                    error("benchmark directory '" + benchmark_dir + "' does not exist!")
                    exit(1)
                last_benchmark = benchmark_arg
                benchmark_dirs.append(benchmark_dir)
                name = os.path.basename(os.path.normpath(benchmark_root))
                benchmark_names[benchmark_dir] = name
            else:
                benchmark_dir = benchmark_arg
                if not os.path.exists(benchmark_dir):
                    error("benchmark directory '" + benchmark_dir + "' does not exist!")
                    exit(1)
                last_benchmark = benchmark_arg
                benchmark_dirs.append(benchmark_dir)
                name = os.path.basename(os.path.normpath(benchmark_dir))
                benchmark_names[benchmark_dir] = name

        for benchmark_dir in benchmark_dirs:
            print()
            config.runs[benchmark_names[benchmark_dir]] = benchmark_dir

        # Make the benchmarks
        benchmark_id = 0
        self.benchmarks = []
        for benchmark_dir in benchmark_dirs:
            benchmark_id += 1
            benchmark = Benchmark(benchmark_dir, benchmark_names[benchmark_dir], benchmark_id, config)
            benchmark.lookup_period()
            # benchmark.init_files()
            # benchmark.load_workers()
            # self.benchmarks.append(benchmark)

        prepare(config)
        df = analyze(config)
        report(config, df)

    def output_dir(self, name):
        output_dir = os.path.join(report_dir, name)
        mkdir(output_dir)
        return output_dir

    # makes the actual comparison report.
    def make(self):
        info("Done writing report [" + report_dir + "]")
        for benchmark in self.benchmarks:
            info(" benchmark [" + benchmark.name + "] benchmark.dir [" + benchmark.src_dir + "]")


class HTMLReport:

    def __init__(self):
        self.report = ""
        self.images = ""
        self.metrics = []

    def addImage(self, plot):
        if plot.skipped:
            return

        metric_name = plot.image_path.split('/')[-2]

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
        try:
            for i in range(len(csvContents[0].split(','))):
                self.report = self.report + '<tr>'
                for j in range(len(csvContents)):
                    self.report = self.report + '<td>' + csvContents[j].split(',')[i].replace('"', '') + '</td>'
                self.report = self.report + '</tr>'
        except IndexError:
            # We need on this problem in the future.
            pass
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

        file_url = "file://" + file_name
        info(f"HTML report generated at: {file_url}")


class PerfTestReportCli:
    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Creating a benchmark report from one or more benchmarks.')
        parser.add_argument('benchmarks',
                            metavar='B',
                            nargs='+',
                            help='a benchmark to be used in the comparison')
        # parser.add_argument('-r', '--realtime', default='report', help='print the real time of the datapoints.')
        parser.add_argument('-o', '--output',
                            nargs=1,
                            help='The output directory for the report. By hazelcast4 a report directory in the working directory is created.')
        parser.add_argument('-w', '--warmup',
                            nargs=1, default=[0],
                            type=int,
                            help='The warmup period in seconds. The warmup removes datapoints from the start.')
        parser.add_argument('-c', '--cooldown',
                            nargs=1, default=[0],
                            type=int,
                            help='The cooldown period in seconds. The cooldown removes datapoints from the end.')
        parser.add_argument('-f', '--full',
                            help='Enable individual worker level diagrams.',
                            action="store_true")
        parser.add_argument('-l', '--last',
                            help='Compare last results from each benchmark',
                            action='store_true')
        parser.add_argument('--svg',
                            help='SVG instead of PNG graphics.',
                            action="store_true")
        parser.add_argument('--width',
                            nargs=1,
                            default=[1600],
                            type=int,
                            help='The width in pixels of the generated images.')
        parser.add_argument('--height',
                            nargs=1,
                            default=[1200],
                            type=int,
                            help='The height in pixels of the generated images.')
        global args
        args = parser.parse_args(argv)
        global benchmark_args
        benchmark_args = args.benchmarks

        gc_logs_found = False

        os.environ['LC_CTYPE'] = "en_US.UTF-8"

        global report_dir
        if not args.output:
            report_dir = "report"
        else:
            report_dir = args.output[0]
        report_dir = os.path.abspath(report_dir)

        config = ReportConfig(report_dir)
        config.warmup_seconds = int(args.warmup[0])
        config.cooldown_seconds = int(args.cooldown[0])
        config.image_width_px = int(args.width[0])
        config.image_height_px = int(args.height[0])

        global compare_last
        compare_last = args.last

        info("Report directory '" + report_dir + "'")

        if os.path.isdir('report'):
            shutil.rmtree('report')
        global htmlReport
        htmlReport = HTMLReport()
        comparison = Comparison(config)
        htmlReport.generate()

        if not args.full and gc_logs_found:
            info("gc.log files have been found. Run with -f option to get these plotted.")
