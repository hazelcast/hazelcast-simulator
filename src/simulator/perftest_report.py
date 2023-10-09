#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import glob
import shutil
import base64
from pathlib import Path

import simulator.util
from simulator.log import info, error
from simulator.perftest_report_dstat import report_dstat, analyze_dstat
from simulator.perftest_report_hdr import report_hdr, prepare_hdr, analyze_latency_history
from simulator.perftest_report_operations import report_operations, prepare_operation, analyze_operations
from simulator.util import simulator_home, read, write, mkdir
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
        self.image_list = []
        self.metrics = []

    def addImage(self, type, path, title):
        self.image_list.append((type, path, title))

    def loadReportCsv(self):
        contents = []
        with open(os.path.join(report_dir + "/report.csv")) as csvfile:
            line = csvfile.readline()
            while line != '':
                contents.append(line)
                line = csvfile.readline()

        return contents

    def importImages(self):
        dstat_dir = f"{report_dir}/dstat"
        for agent_filename in os.listdir(dstat_dir):
            agent_dir = f"{dstat_dir}/{agent_filename}"
            for image_filename in os.listdir(agent_dir):
                if not image_filename.endswith(".png"):
                    continue
                base_image_filename = Path(image_filename).stem
                self.addImage("dstat",
                              f"{agent_dir}/{image_filename}",
                              f"{agent_filename} {base_image_filename}")

        for path in Path(f"{report_dir}/latency").rglob('*.png'):
            htmlReport.addImage("latency", path.resolve().as_posix(), "")

        for path in Path(f"{report_dir}/operations").rglob('*.png'):
            htmlReport.addImage("operations", path.resolve().as_posix(), "")

    def generate(self):
        report_csv = self.loadReportCsv()

        html_template = read(f"{simulator_home}/src/simulator/report.html")
        file_path = os.path.join(report_dir + "/report.html")
        file_url = "file://" + file_path
        info(f"Generating HTML report : {file_url}")

        images_index = html_template.index("[images]")
        overview_index = html_template.index("[overview]")
        with open(file_path, 'w') as f:
            f.write(html_template[0:images_index])

            for (type, path, title) in self.image_list:
                metric_name = path.split('/')[-2]

                with open(path, "rb") as image_file:
                    encoded_image = str(base64.b64encode(image_file.read()), encoding='utf-8')

                encoded_image = "data:image/png;base64,%s" % encoded_image

                # todo: flatten into multiline string
                image_html = '<div class="image-container ' + type + '">'
                image_html = image_html + '<img src="' + encoded_image + '" onclick="toggleZoom(this);" />'
                image_html = image_html + '<p class="image-text">' + path + "</p>"
                image_html = image_html + '</div>'
                f.write(image_html)

            f.write(html_template[images_index + len("[images]"):overview_index])
            overview = ""
            try:
                for i in range(len(report_csv[0].split(','))):
                    overview = overview + '<tr>'
                    for j in range(len(report_csv)):
                        overview = overview + '<td>' + report_csv[j].split(',')[i].replace('"', '') + '</td>'
                    overview = overview + '</tr>'
            except IndexError:
                # We need on this problem in the future.
                pass
            f.write(overview)
            f.write(html_template[overview_index + len("[overview]"):])


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
                            help='The width, in pixels, of the generated images.')
        parser.add_argument('--height',
                            nargs=1,
                            default=[1200],
                            type=int,
                            help='The height, in pixels, of the generated images.')
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
        htmlReport.importImages()
        htmlReport.generate()

        if not args.full and gc_logs_found:
            info("gc.log files have been found. Run with -f option to get these plotted.")
