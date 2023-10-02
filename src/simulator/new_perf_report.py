import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import matplotlib
import pandas
from matplotlib import mlab
import numpy as np
import pandas as pd
from matplotlib.ticker import StrMethodFormatter

pd.options.mode.chained_assignment = None
import util
from simulator.log import log
from typing import Dict, Tuple

# from util import write
image_dpi = 96
image_width_px = 1600
image_height_px = 1200
import matplotlib.pyplot as plt
import time
import os


def multiple_by(df, amount, *column_names):
    for column_name in column_names:
        column = df[column_name]
        values = len(column.values)
        for i in range(values):
            column.iloc[i] = amount * column.iloc[i]


def mkdir(path):
    from pathlib import Path
    Path(path).mkdir(parents=True, exist_ok=True)


def to_relative_time(df, time_column_name):
    column = df[time_column_name]
    base = None
    for i in range(len(column.values)):
        if not base:
            base = column.iloc[i]
        column.iloc[i] = column.iloc[i] - base


def sizeof_fmt(x, pos):
    if x < 0:
        return ""
    for x_unit in ['bytes', 'kB', 'MB', 'GB', 'TB']:
        if x < 1024.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1024.0


class ColumnTitle:
    seperator = "::"
    kv_seperator = "=="

    def __init__(self, group, metric, attributes=None):
        if attributes is None:
            attributes = {}
        self.group = group
        self.metric_id = metric
        self.attributes = attributes

    def to_string(self):
        result = f"{self.group}{ColumnTitle.seperator}{self.metric_id}"

        if self.attributes is not None:
            for key, value in self.attributes.items():
                if not value is None:
                    result = result + f"{ColumnTitle.seperator}{key}{ColumnTitle.kv_seperator}{value}"
        return result

    @staticmethod
    def from_string(column_name):
        args = column_name.split(ColumnTitle.seperator)
        group = args[0]
        metric = args[1]
        attributes = {}
        for k in range(2, len(args)):
            pair = args[k].split(ColumnTitle.kv_seperator)
            attributes[pair[0]] = pair[1]

        return ColumnTitle(group, metric, attributes)


def merge_on_time(*frames):
    result = None
    for frame in frames:
        if frame is None:
            continue
        elif result is None:
            result = frame
        else:
            result = pd.concat([result, frame], axis=1, join="outer")
    return result


#
# def merge_on_time(df_a, df_b):
#     if df_a is None:
#         return df_b
#     else:
#
#         result = pd.concat([df_a, df_b], axis=1, join="outer")


def find_worker_id(path):
    if not os.path.isdir(path):
        return None

    worker_name = os.path.basename(path)
    if not worker_name.startswith("A"):
        return None

    index = worker_name.index("-")
    return worker_name[:index]


def log_section(text):
    print("---------------------------------------------------")
    print(text)
    print("---------------------------------------------------")



def plot_latency_history(report_dir, df):
    log_section("Plotting latency history: Start")
    start_sec = time.time()

    grouped_column_names = {}
    for column_name in df.columns:
        column_title = ColumnTitle.from_string(column_name)
        if column_title.group != "Latency":
            continue
        if column_title.metric_id == "StartTime" or column_title.metric_id == "Timestamp":
            continue
        metric_id = column_title.metric_id
        worker_id = column_title.attributes.get("worker_id")
        test_id = column_title.attributes.get("test_id")
        column_names = grouped_column_names.get((worker_id, metric_id, test_id, worker_id))
        if column_names is None:
            column_names = []
            grouped_column_names[(worker_id, metric_id, test_id, worker_id)] = column_names
        column_names.append(column_name)

    for (worker_id, metric_id, test_id, worker_id), column_name_list in grouped_column_names.items():
        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

        for column_name in column_name_list:
            column_title = ColumnTitle.from_string(column_name)
            plt.plot(df.index, df[column_name], label=column_title.attributes["run_id"])

        plt.ticklabel_format(style='plain', axis='y')
        plt.title(metric_id.replace('_', ' '))
        if metric_id == "Int_Throughput" or metric_id == "Total_Throughput":
            plt.ylabel("operations/second")
        elif metric_id == "Inc_Count" or metric_id == "Total_Count":
            plt.ylabel("operations")
        else:
            plt.ylabel("microseconds")

        plt.xlabel("Time")
        # plt.gca.xaxis.set_major_formatter(mdates.DateFormatter('%m-%S'))
        plt.legend()
        # if worker_id == '':
        #    plt.title(f"{test_id} {column_name.capitalize()}")
        # else:

        if test_id is None:
            test_str = ""
        else:
            test_str = f"-{test_id}"

        plt.grid()

        if worker_id is None:
            path = f"{report_dir}/{metric_id}{test_str}.png"
        else:
            dir = f"{report_dir}/{worker_id}"
            mkdir(dir)
            path = f"{dir}/{metric_id}{test_str}.png"
        print(f"\tGenerating [{path}]")
        plt.savefig(path)
        plt.close()

    duration_sec = time.time() - start_sec
    log_section(f"Plotting latency history: Done (duration {duration_sec:.2f} seconds)")


def collect_dstat(run_dir, attributes):
    log_section("Loading dstat data: Start")
    start_sec = time.time()

    result = None
    for csv_filename in os.listdir(run_dir):
        if not csv_filename.endswith("_dstat.csv"):
            continue
        agent_id = csv_filename[:csv_filename.index("_")]
        csv_path = f"{run_dir}/{csv_filename}"
        print(f"\tLoading {csv_path}")
        df = pd.read_csv(csv_path, skiprows=5)
        multiple_by(df, 1000, 'used', 'free', 'buf', 'cach')

        df['time'] = pd.to_datetime(df['epoch'], unit='s')
        df.set_index('time', inplace=True)
        df.drop(['epoch'], inplace=True, axis=1)

        new_attributes = attributes.copy()
        new_attributes["agent_id"] = agent_id
        for column_name in df.columns:
            if column_name == "time":
                continue

            column_title = ColumnTitle("dstat", column_name, new_attributes)
            df.rename(columns={column_name: column_title.to_string()}, inplace=True)

        result = merge_on_time(result, df)
        # if not absolute_time:
        #     to_relative_time(df, "epoch")
        # df['epoch'] = pd.to_datetime(df['epoch'], unit='s')

    duration_sec = time.time() - start_sec
    log_section(f"Loading dstat data: Done (duration {duration_sec:.2f} seconds)")
    return result


def plot_dstat(report_dir, df):
    log_section("Plotting dstat data: Start")
    start_sec = time.time()

    grouped_column_names = {}
    for column_name in df.columns:
        column_title = ColumnTitle.from_string(column_name)
        if column_title.group != "dstat":
            continue
        metric_id = column_title.metric_id
        agent_id = column_title.attributes["agent_id"]
        column_names = grouped_column_names.get((agent_id, metric_id))
        if column_names is None:
            column_names = []
            grouped_column_names[(agent_id, metric_id)] = column_names
        column_names.append(column_name)

    for (agent_id, metric_id), column_name_list in grouped_column_names.items():
        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
        for column_name in column_name_list:
            column_title = ColumnTitle.from_string(column_name)
            plt.plot(df.index, df[column_name], label=column_title.attributes["run_id"])

        plt.ticklabel_format(style='plain', axis='y')
        plt.ylabel(metric_id)
        plt.xlabel("Time")
        # plt.gca.xaxis.set_major_formatter(mdates.DateFormatter('%m-%S'))
        plt.legend()

        plt.title(f"Agent {agent_id} : {metric_id}")
        plt.grid()

        nice_metric_name = column_title.metric_id.replace("/", "_").replace(":", "_")
        path = f"{report_dir}/{agent_id}_{nice_metric_name}.png"

        print(f"\tGenerating [{path}]")
        plt.savefig(path)
        plt.close()

    duration_sec = time.time() - start_sec
    log_section(f"Plotting dstat data: Done (duration {duration_sec:.2f} seconds)")


dstat_titles = {"1m": "1 Minute load average",
                "5m": "5 Minute load average",
                "15m": "15 minute load average",
                "cach": "Memory Usage: Cached",
                "buf": "Memory Usage: Buffered",
                "free": "Memory Usage: Free",
                "used": "Memory Usage: Used"}


def load_latency_distribution(run_dir):
    result = {}
    for dir_name in os.listdir(run_dir):
        worker_dir = f"{run_dir}/{dir_name}"
        worker_id = find_worker_id(worker_dir)
        if not worker_id:
            continue

        for file_name in os.listdir(worker_dir):
            if not file_name.endswith(".hgrm"):
                continue
            csv_path = f"{worker_dir}/{file_name}"
            if not os.path.exists(csv_path):
                continue
            print(f"\tLoading {csv_path}")
            df = pd.read_csv(csv_path, delim_whitespace=True, comment='#')

            total_count_column = df['TotalCount']
            count = []
            for i in range(len(total_count_column.values)):
                if len(count) == 0:
                    count.append(total_count_column.iloc[i])
                else:
                    count.append(total_count_column.iloc[i] - total_count_column.iloc[i - 1])
            df['Count'] = count
            result[worker_id] = df
    return result


# def plot_cumulative_latency_distribution(report_dir, *df_workers_list):
#     log("Plotting cumulative latency distribution data")
#
#     first_df_workers = df_workers_list[0]
#     for worker_name, first_df in first_df_workers.items():
#         result_dir = f"{report_dir}/latency/{worker_name}"
#         os.makedirs(result_dir, exist_ok=True)
#
#         fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
#         # plt.yscale('log')
#         plt.xscale('log')
#         axes = plt.gca()
#
#         axes.set_xticks([1, 10, 100, 1000, 10000, 100000, 1000000, 10000000])
#         axes.set_xticklabels(["0%", "90%", "99%", "99.9%", "99.99%", "99.999%", "99.9999%", "99.99999%"])
#         plt.plot(first_df['1/(1-Percentile)'], first_df['Value'])
#         for i in range(1, len(df_workers_list)):
#             other_df_worker = df_workers_list[i]
#             if not worker_name in other_df_worker:
#                 continue
#
#             other_df = other_df_worker[worker_name]
#             plt.plot(other_df['1/(1-Percentile)'], other_df['Value'])
#
#         plt.grid()
#         plt.savefig(f'{result_dir}/cumulative_latency_distribution.png')
#         plt.close()
#
#     log("Plotting cumulative latency distribution data: done")

#
# def plot_latency_histogram(report_dir, *df_workers_list):
#     first_df_workers = df_workers_list[0]
#     for worker_name, first_df in first_df_workers.items():
#         result_dir = f"{report_dir}/latency/{worker_name}"
#
#         os.makedirs(result_dir, exist_ok=True)
#         fig = plt.figure(figsize=(2 * image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
#         first_df.plot(kind='bar', x='Value', y="Count", ax=plt.gca())
#         for i in range(1, len(df_workers_list)):
#             other_df_worker = df_workers_list[i]
#             if not worker_name in other_df_worker:
#                 continue
#
#             other_df = other_df_worker[worker_name]
#             other_df.plot(kind='bar', x='Value', y="Count", ax=plt.gca())
#         plt.xlabel("Latency (microseconds)")
#         plt.ylabel("Count")
#         plt.yscale('log')
#
#         plt.grid()
#         plt.savefig(f'{result_dir}/latency_histogram.png')
#         plt.close()


absolute_time = True
warmup_seconds = 5
cooldown_seconds = 5


def collect_operations_data(run_dir, attributes):
    log_section("Loading operations data: Start")
    start_sec = time.time()
    rename_performance_csv(run_dir)
    create_aggregated_operations_csv(run_dir)

    df_list = []
    df_list.extend(load_aggregated_operations_csv(run_dir, attributes))
    df_list.extend(load_worker_operations_csv(run_dir, attributes))

    result = None
    for df in df_list:
        result = merge_on_time(result, df)

    duration_sec = time.time() - start_sec
    log_section(f"Loading operations data: Done (duration {duration_sec:.2f} seconds)")

    return result


def load_worker_operations_csv(run_dir, attributes):
    result = []
    # load the df of the workers.
    for outer_file in os.listdir(run_dir):
        worker_dir = f"{run_dir}/{outer_file}"
        worker_id = find_worker_id(worker_dir)

        if not worker_id:
            continue

        for inner_file_name in os.listdir(worker_dir):
            if not inner_file_name.startswith("operations-") or not inner_file_name.endswith(".csv"):
                continue

            test_id = inner_file_name.replace("operations-", "").replace(".csv", "")
            csv_path = f"{worker_dir}/{inner_file_name}"
            print(f"\tLoading {csv_path}")
            df = pd.read_csv(csv_path)
            if len(df.index) == 0:
                continue

            # we need to round the epoch time to the nearest second
            df['time'] = df['epoch'].round(0).astype(int)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df.set_index('time', inplace=True)
            df.drop(['epoch'], inplace=True, axis=1)
            df.drop(['timestamp'], inplace=True, axis=1)
            new_attributes = attributes.copy()
            new_attributes["test_id"] = test_id
            new_attributes["worker_id"] = worker_id
            for column_name in df.columns:
                if column_name == "time":
                    continue
                column_title = ColumnTitle("Operations", column_name, new_attributes)
                df.rename(columns={column_name: column_title.to_string()}, inplace=True)
            result.append(df)

    return result


# Renames the old performance csv files to operations csv files
def rename_performance_csv(run_dir):
    for outer_file_name in os.listdir(run_dir):
        outer_dir = f"{run_dir}/{outer_file_name}"
        worker_id = find_worker_id(outer_dir)

        if not worker_id:
            continue

        for inner_file_name in os.listdir(outer_dir):
            if not inner_file_name.startswith("performance") or not inner_file_name.endswith(".csv"):
                continue

            name = inner_file_name.replace("performance", "").replace(".csv", "")
            shutil.copyfile(f"{outer_dir}/{inner_file_name}", f"{outer_dir}/operations{name}.csv")


def load_aggregated_operations_csv(run_dir, attributes):
    result = []
    # load the aggregated performance data
    for file_name in os.listdir(run_dir):
        if not file_name.startswith("operations") or not file_name.endswith(".csv"):
            continue
        if file_name.startswith("operations-"):
            test_id = file_name.replace("operations-", "").replace(".csv", "")
        else:
            test_id = None
        csv_path = f"{run_dir}/{file_name}"
        print(f"\tLoading {csv_path}")
        df = pd.read_csv(csv_path)
        if len(df.index) == 0:
            continue

        df['time'] = df['epoch'].round(0).astype(int)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.set_index('time', inplace=True)
        df.drop(['epoch'], inplace=True, axis=1)
        df.drop(['timestamp'], inplace=True, axis=1)

        new_attributes = attributes.copy()
        new_attributes["test_id"] = test_id
        for column_name in df.columns:
            if column_name == "time":
                continue
            column_title = ColumnTitle("Operations", column_name, new_attributes)
            df.rename(columns={column_name: column_title.to_string()}, inplace=True)

        result.append(df)
    return result


def create_aggregated_operations_csv(run_dir):
    df_list_map = {}

    # load all the operation datafromes for every worker/test
    for outer_file_name in os.listdir(run_dir):
        outer_dir = f"{run_dir}/{outer_file_name}"
        worker_id = find_worker_id(outer_dir)
        if not worker_id:
            continue

        for inner_file_name in os.listdir(outer_dir):
            if not inner_file_name.startswith("operations") or not inner_file_name.endswith(".csv"):
                continue
            test_id = inner_file_name.replace("operations", "").replace(".csv", "")
            df_list = df_list_map.get(test_id)
            if df_list is None:
                df_list = []
                df_list_map[test_id] = df_list
            csv_path = f"{outer_dir}/{inner_file_name}"
            print(f"\tLoading {csv_path}")
            df = pd.read_csv(csv_path)
            df['epoch'] = df['epoch'].round(0).astype(int)
            df.set_index('epoch', inplace=True)
            if len(df.index) > 0:
                df_list.append(df)

    # merge the frames into the
    for test_id, df_list in df_list_map.items():
        aggregate_df = df_list[0]
        for df_index in range(1, len(df_list)):
            df = df_list[df_index]
            for ind in df.index:
                row = df.T.get(ind)
                aggregate_row = aggregate_df.T.get(ind)
                if aggregate_row is None:
                    # it is a row with a time that doesn't exist in the aggregate
                    # so the whole row can be added.
                    aggregate_df.loc[ind] = row
                else:
                    # it is a row with a time that already exist. So a new row is
                    # created whereby every value from the 2 rows are added into
                    # a new row and that is written back to the aggregate_df
                    new_row = []
                    for value_ind in range(len(row)):
                        df_value = row.iloc[value_ind]
                        aggregated_df_value = aggregate_row.iloc[value_ind]
                        result = df_value + aggregated_df_value
                        new_row.append(result)
                    aggregate_df.loc[ind] = new_row

        aggregate_df.to_csv(f"{run_dir}/operations{test_id}.csv")


def processing_hdr(dir):
    log_section("Processing hdr files: Start")
    start_sec = time.time()

    merge_worker_hdr(dir)

    for hdr_file in list(Path(dir).rglob("*.hdr")):
        hdr_file_name_no_ext = Path(hdr_file).stem
        hdr_file_dir = Path(hdr_file).parent
        if os.path.exists(f"{hdr_file_dir}/{hdr_file_name_no_ext}.latency-history.csv"):
            continue
        util.shell(f"""java -cp "{util.simulator_home}/lib/*" \
                    com.hazelcast.simulator.utils.SimulatorHistogramLogProcessor \
                    -i {hdr_file} \
                    -o {hdr_file_dir}/{hdr_file_name_no_ext} \
                    -outputValueUnitRatio 1000""")
        os.rename(f"{hdr_file_dir}/{hdr_file_name_no_ext}.hgrm", f"{hdr_file_dir}/{hdr_file_name_no_ext}.hgrm.bak")
        util.shell(f"""java -cp "{util.simulator_home}/lib/*" \
                       com.hazelcast.simulator.utils.SimulatorHistogramLogProcessor \
                       -csv \
                       -i {hdr_file} \
                       -o {hdr_file_dir}/{hdr_file_name_no_ext} \
                       -outputValueUnitRatio 1000""")
        os.rename(f"{hdr_file_dir}/{hdr_file_name_no_ext}.hgrm.bak", f"{hdr_file_dir}/{hdr_file_name_no_ext}.hgrm")
        os.rename(f"{hdr_file_dir}/{hdr_file_name_no_ext}",
                  f"{hdr_file_dir}/{hdr_file_name_no_ext}.latency-history.csv")

    duration_sec = time.time() - start_sec
    log_section(f"Processing hdr files: Done {duration_sec:.2f} seconds)")
    pass


def merge_worker_hdr(dir):
    dic = {}
    for worker_dir_name in os.listdir(dir):
        dir_path = f"{dir}/{worker_dir_name}"
        worker_name = find_worker_id(dir_path)
        if not worker_name:
            continue

        for file_name in os.listdir(dir_path):
            if not file_name.endswith(".hdr"):
                continue

            hdr_file = f"{dir}/{worker_dir_name}/{file_name}"
            files = dic.get(file_name)
            if not files:
                files = []
                dic[file_name] = files
            files.append(hdr_file)

    for file_name, hdr_files in dic.items():
        util.shell(f"""java -cp "{util.simulator_home}/lib/*" \
                         com.hazelcast.simulator.utils.HistogramLogMerger \
                         {dir}/{file_name} {" ".join(hdr_files)} 2>/dev/null""")


def collect_latency_history(run_dir, attributes):
    log_section("Loading latency history data: Start")
    start_sec = time.time()
    result = None

    # iterate over the files in the run directory
    for outer_file_name in os.listdir(run_dir):
        outer_path = f"{run_dir}/{outer_file_name}";
        if outer_file_name.endswith(".latency-history.csv"):
            csv_df = load_latency_history_csv(outer_path, attributes, None)
            result = merge_on_time(result, csv_df)
        else:
            worker_id = find_worker_id(outer_path)

            if not worker_id:
                continue

            # iterate over the files in the worker directory
            for inner_file_name in os.listdir(outer_path):
                if not inner_file_name.endswith(".latency-history.csv"):
                    continue

                csv_df = load_latency_history_csv(
                    f"{outer_path}/{inner_file_name}", attributes, worker_id)
                result = merge_on_time(result, csv_df)

    duration_sec = time.time() - start_sec
    log_section(f"Loading latency history data: Done (duration {duration_sec:.2f} seconds)")
    return result


def load_latency_history_csv(file_path, attributes, worker_id):
    test_id = os.path.basename(file_path).replace(".latency-history.csv", "")
    print(f"\tLoading {file_path}")
    df = pd.read_csv(file_path, skiprows=2)

    for column_name in df.columns:
        if column_name.startswith("Unnamed"):
            df.drop([column_name], inplace=True, axis=1)

    df['time'] = df['StartTime'].round(0).astype(int)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    # print(csv_df)

    new_attributes = attributes.copy()
    new_attributes["test_id"] = test_id
    new_attributes["worker_id"] = worker_id
    for column_name in df.columns:
        if column_name == "time":
            continue

        metric = column_name
        if metric == "Total_Min%":
            metric = "Total_Min"
        elif "%" in metric:
            metric = metric.replace("%", "")
            if metric.startswith("Int_"):
                metric = metric.replace("Int_", "Int_p")
            elif metric.startswith("Total_"):
                metric = metric.replace("Total_", "Total_p")
        column_title = ColumnTitle("Latency", metric, new_attributes)
        df.rename(columns={column_name: column_title.to_string()}, inplace=True)
    return df


@dataclass
class Period:
    start: int
    end: int


@dataclass
class Run:
    id: str
    path: str
    date: datetime
    worker_periods: Dict[Tuple[str, str], Period]


# todo: each run of a benchmark should be a single dataframe
class Benchmark:

    def __init__(self, name, path):
        self.runs = []
        self.name = name
        self.path = path
        self.is_simple = False

        run_paths = []
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            if not os.path.isdir(file_path):
                continue
            if file_name.startswith("A") and "W" in file_name:
                self.is_simple = True
                break
            else:
                run_paths.append(file_path)

        if self.is_simple:
            run = Run("foo", path, None, {})
            self.runs.append(run)
        else:
            for run_path in run_paths:
                basename = os.path.basename(run_path)
                date = datetime.strptime(basename, '%d-%m-%Y_%H-%M-%S')
                run = Run("foo", run_path, date, {})
                self.runs.append(run)

    def latest_run_path(self):
        run_path_len = len(self.runs)
        if run_path_len == 0:
            return None
        elif run_path_len == 1:
            return self.runs[0]
        else:
            latest_run = None
            latest_date = None
            for run in self.runs:
                if latest_date is None or latest_date < run.date:
                    latest_run = run
                    latest_date = run.date
            return latest_run

    def run_count(self):
        return len(self.runs)


def plot_operations(report_dir, df):
    log_section("Plotting operations data: Start")
    start_sec = time.time()

    grouped_column_names = {}
    for column_name in df.columns:
        column_title = ColumnTitle.from_string(column_name)
        if column_title.group != "Operations" or column_title.metric_id != "operations/second":
            continue
        metric_id = column_title.metric_id
        worker_id = column_title.attributes.get("worker_id")
        test_id = column_title.attributes.get("test_id")
        column_names = grouped_column_names.get((worker_id, metric_id, test_id, worker_id))
        if column_names is None:
            column_names = []
            grouped_column_names[(worker_id, metric_id, test_id, worker_id)] = column_names
        column_names.append(column_name)

    for (worker_id, metric_id, test_id, worker_id), column_name_list in grouped_column_names.items():

        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

        for column_name in column_name_list:
            column_title = ColumnTitle.from_string(column_name)
            plt.plot(df.index, df[column_name], label=column_title.attributes["run_id"])

        plt.ticklabel_format(style='plain', axis='y')
        plt.ylabel("operations/second")
        plt.xlabel("Time")

        plt.legend()
        # if worker_id == '':
        #    plt.title(f"{test_id} {column_name.capitalize()}")
        # else:

        if test_id is None:
            test_str = ""
        else:
            test_str = f"-{test_id}"

        plt.title(f"Throughput")
        plt.grid()

        if worker_id is None:
            path = f"{report_dir}/throughput{test_str}.png"
        else:
            dir = f"{report_dir}/{worker_id}"
            mkdir(dir)
            path = f"{dir}/throughput{test_str}.png"

        print(f"\tGenerating [{path}]")
        plt.savefig(path)
        plt.close()

    duration_sec = time.time() - start_sec
    log_section(f"Plotting operations data: Done (duration {duration_sec:.2f} seconds)")


# def to_start(df):
#     if len(df.index) == 0:
#         return
#
#     index = df.index

def write_xlsx(df):
    path_excel = f"{report_dir}/data.xlsx"
    print(f"path excel: {path_excel}")
    df.to_excel(path_excel)


def write_csv(df):
    path_csv = f"{report_dir}/data.csv"
    print(f"path csv: {path_csv}")
    df.to_csv(path_csv)


start_sec = time.time()

benchmark_htable = Benchmark("htable", "/mnt/home/pveentjer/1000M/verification")
# benchmark_htable = Benchmark("htable", "/home/pveentjer/cpu_1_affinity_1")
benchmark_htable.latest_run_path()

# benchmark_map = Benchmark("map", "/eng/Hazelcast/alto-testing/alto-new-testing/runs/htable/read_only/1KB/cpu_1/")
# print(benchmark_map.run_count())

report_dir = "/mnt/home/pveentjer/report/"  # tempfile.mkdtemp()
mkdir(report_dir)
print(f"Report directory {report_dir}")

run_path = benchmark_htable.latest_run_path()
run_id = "banana"
attributes = {"run_id": run_id}
print(f"Analyzing run_path:{run_path.path}")

df_operations = collect_operations_data(run_path.path, attributes)

processing_hdr(run_path.path)
df_latency_history = collect_latency_history(run_path.path, attributes)

df_dstat = collect_dstat(run_path.path, attributes)

df = merge_on_time(df_operations, df_latency_history, df_dstat)

write_xlsx(df)
write_csv(df)

for column_name in df.columns:
    print(column_name)

plot_operations(report_dir, df_operations)
plot_latency_history(report_dir, df_latency_history)
plot_dstat(report_dir, df_dstat)

duration_sec = time.time() - start_sec
log(f"Generating report: Done  (duration {duration_sec:.2f} seconds)")
