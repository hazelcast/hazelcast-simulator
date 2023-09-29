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
from matplotlib import pyplot as plt
import tempfile
import util
from simulator.log import log
from typing import Dict, Tuple

# from util import write
image_dpi = 96
image_width_px = 1600
image_height_px = 1200
import matplotlib.pyplot as plt, mpld3
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import time
import matplotlib.dates as mdates
import os
from glob import glob


def multiple_by(df, amount, *column_names):
    for column_name in column_names:
        column = df[column_name]
        values = len(column.values)
        for i in range(values):
            column.iloc[i] = amount * column.iloc[i]


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
        self.metric = metric
        self.attributes = attributes

    def to_string(self):
        result = f"{self.group}{ColumnTitle.seperator}{self.metric}"

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


def inner_join(df_a, df_b):
    if df_a is None:
        return df_b
    else:
        return pd.concat([df_a, df_b], axis=1, join="inner")


def find_worker_id(path):
    if not os.path.isdir(path):
        return None

    workername = os.path.basename(path)
    if not workername.startswith("A"):
        return None

    index = workername.index("-")
    return workername[:index]


#
#
# def aggregate_performance_data(perf_data):
#     log("Aggregating performance data")
#
#     delta_map = {}
#     epochs_data = []
#     for df in perf_data.values():
#         epoch_column = df['epoch']
#         delta_column = df['operations-delta']
#         for i in range(len(epoch_column.values)):
#             epoch = epoch_column.iloc[i]
#
#             if epoch in delta_map:
#                 delta_map[epoch] = delta_column.iloc[i] + delta_map[epoch]
#             else:
#                 epochs_data.append(epoch)
#                 delta_map[epoch] = delta_column.iloc[i]
#
#     operations_delta_data = []
#     operations_data = []
#     last_operations = None
#     last_epoch = None
#     epochs_data.sort()
#     for epoch in epochs_data:
#         if not last_epoch:
#             last_epoch = epoch
#             last_operations = 0
#
#         delta = delta_map[epoch]
#         operations_delta_data.append(delta)
#         last_operations += delta
#         operations_data.append(last_operations)
#
#     df = pd.DataFrame({'epoch': epochs_data,
#                        'operations': operations_data,
#                        'operations-delta': operations_delta_data,
#                        'operations/second': operations_delta_data})
#     # print(df)
#     perf_data[''] = df
#     log("Aggregating performance data: done")


def plot_latency_history(report_dir, latency_history_data_runs):
    df_workers_list = list(latency_history_data_runs.values())

    first_df_workers = df_workers_list[0]
    for worker_name, first_df in first_df_workers.items():
        result_dir = f"{report_dir}/latency/{worker_name}"
        os.makedirs(result_dir)

        for column_index in range(2, len(first_df.columns)):
            column_name = first_df.columns[column_index]
            if column_name.startswith("Unnamed"):
                continue

            fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

            plt.plot(first_df['epoch'], first_df[column_name])
            for i in range(1, len(df_workers_list)):
                other_df_worker = df_workers_list[i]
                if not worker_name in other_df_worker:
                    continue

                other_df = other_df_worker[worker_name]
                if not column_name in other_df:
                    continue
                plt.plot(other_df['epoch'], other_df[column_name])

            filename = column_name.replace("/", "_")
            plt.grid()
            if column_name == "Total_Count":
                plt.ylabel("Count")
            elif "Throughput" in column_name:
                plt.ylabel("Throughput (operations/second)")
            else:
                plt.ylabel("Latency (microseconds)")
            plt.xlabel("Time")

            pretty_column_name = column_name.replace("_", " ")
            if pretty_column_name.startswith("Int "):
                pretty_column_name = pretty_column_name.replace("Int ", "Interval ")

            plt.title(f"{worker_name} - {pretty_column_name}")
            plt.savefig(f'{result_dir}/{filename}.png')
            plt.close()


def plot_latency_history(report_dir, df):
    for column_name in df.columns:
        column_title = ColumnTitle.from_string(column_name)
        if column_title.group != "Latency":
            continue
        metric = column_title.metric
        if metric == "StartTime" or metric == "Timestamp":
            continue

        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

        plt.plot(df.index, df[column_name], label=column_name)

        plt.ticklabel_format(style='plain', axis='y')
        plt.title(metric.replace('_',' '))
        plt.ylabel("us")
        plt.xlabel("Time")
            # plt.gca.xaxis.set_major_formatter(mdates.DateFormatter('%m-%S'))
        plt.legend()
        #if worker_id == '':
        #    plt.title(f"{test_id} {column_name.capitalize()}")
        #else:

        test_id = column_title.attributes.get("test_id")
        if test_id is None:
            test_str = ""
        else:
            test_str = f"-{test_id}"

        worker_id = column_title.attributes.get("worker_id")

        plt.grid()

        if worker_id is None:
            path = f"{report_dir}/{metric}{test_str}.png"
        else:
            dir =  f"{report_dir}/{worker_id}"
            if not os.path.exists(dir):
                os.mkdir(dir)
            path = f"{dir}/{metric}{test_str}.png"
        print(f"Generating [{path}]")
        plt.savefig(path)
        plt.close()

# def load_dstat(benchmark_dir):
#     result = {}
#     for csv_filename in os.listdir(benchmark_dir):
#         if not csv_filename.endswith("_dstat.csv"):
#             continue
#         agent_name = csv_filename[:csv_filename.index("_")]
#         csv_path = f"{benchmark_dir}/{csv_filename}"
#         df = pd.read_csv(csv_path, skiprows=5)
#         multiple_by(df, 1000, 'used', 'free', 'buf', 'cach')
#         # if not absolute_time:
#         #     to_relative_time(df, "epoch")
#         # df['epoch'] = pd.to_datetime(df['epoch'], unit='s')
#         result[agent_name] = df
#     return result


dstat_titles = {"1m": "1 Minute load average",
                "5m": "5 Minute load average",
                "15m": "15 minute load average",
                "cach": "Memory Usage: Cached",
                "buf": "Memory Usage: Buffered",
                "free": "Memory Usage: Free",
                "used": "Memory Usage: Used"}


# def plot_dstat(report_dir, data_runs):
#     df_per_agent_per_run = {}
#     for (agent_id, run_id), df in data_runs.items():
#         df_per_run = df_per_agent_per_run.get(agent_id)
#         if not df_per_run:
#             df_per_run = {}
#             df_per_agent_per_run[agent_id] = df_per_run
#         df_per_run[run_id] = df
#
#     agent_list = list(df_per_agent_per_run.keys())
#     for agent_id in agent_list:
#         df_per_run = df_per_agent_per_run[agent_id]
#         result_dir = f"{report_dir}/dstat/{agent_id}"
#
#         os.makedirs(result_dir)
#         run_id_list = list(df_per_run.keys())
#         print(run_id_list)
#         first_df = df_per_run[run_id_list[0]]
#         for c in range(2, len(first_df.columns)):
#             column_name = first_df.columns[c]
#             fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
#
#             title = dstat_titles.get(column_name)
#             if title:
#                 plt.title(f"{agent_id} - {title} ({column_name})")
#             else:
#                 plt.title(f"{agent_id} - {column_name}")
#
#             for i in range(0, len(run_id_list)):
#                 run_id = run_id_list[i]
#                 df = df_per_run[run_id]
#                 if not column_name in df:
#                     continue
#                 plt.plot(pd.to_datetime(df['epoch'], unit='s'),
#                          df[column_name],
#                          label=agent_id + " " + run_id)
#
#             filename = column_name.replace("/", "_")
#             plt.xlabel("Time")
#             plt.legend()
#             plt.grid()
#             plt.savefig(f'{result_dir}/{filename}.png')
#             plt.close()


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
            df = pd.read_csv(csv_path, delim_whitespace=True, comment='#')

            total_count_column = df['TotalCount']
            count = []
            for i in range(len(total_count_column.values)):
                if len(count) == 0:
                    count.append(total_count_column.iloc[i])
                else:
                    count.append(total_count_column.iloc[i] - total_count_column.iloc[i - 1])
            df['Count'] = count
            # print(df.to_string())
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


def load_operations_data(run_dir):
    rename_performance_csv(run_dir)
    create_aggregated_operations_csv(run_dir)

    df_list = []
    df_list.extend(load_aggregated_operations_csv(run_dir))
    df_list.extend(load_worker_operations_csv(run_dir))

    result = None
    for df in df_list:
        result = inner_join(result, df)

    return result


def load_worker_operations_csv(run_dir):
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
            df = pd.read_csv(csv_path)
            if len(df.index) == 0:
                continue

            # we need to round the epoch time to the nearest second
            df['time'] = df['epoch'].round(0).astype(int)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df.set_index('time', inplace=True)
            df.drop(['epoch'], inplace=True, axis=1)
            df.drop(['timestamp'], inplace=True, axis=1)
            for column_name in df.columns:
                if column_name == "time":
                    continue
                column_title = ColumnTitle("Operations", column_name,
                                           {"test_id": test_id, "worker_id": worker_id})
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


def load_aggregated_operations_csv(run_dir):
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
        df = pd.read_csv(csv_path)
        if len(df.index) == 0:
            continue

        print(df)
        df['time'] = df['epoch'].round(0).astype(int)
        df['time'] = pd.to_datetime(df['time'], unit='s')
        df.set_index('time', inplace=True)
        df.drop(['epoch'], inplace=True, axis=1)
        df.drop(['timestamp'], inplace=True, axis=1)

        for column_name in df.columns:
            if column_name == "time":
                continue
            column_title = ColumnTitle("Operations", column_name, {"test_id": test_id})
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
            df = pd.read_csv(f"{outer_dir}/{inner_file_name}")
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
                    aggregate_df = aggregate_df.append(df.loc[ind])
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


def load_latency_history(run_dir):
    result = None

    # iterate over the files in the run directory
    for outer_file_name in os.listdir(run_dir):
        outer_path = f"{run_dir}/{outer_file_name}";
        if outer_file_name.endswith(".latency-history.csv"):
            csv_df = load_latency_history_csv(outer_path, None)
            result = inner_join(result, csv_df)
        else:
            worker_id = find_worker_id(outer_path)

            if not worker_id:
                continue

            # iterate over the files in the worker directory
            for inner_file_name in os.listdir(outer_path):
                if not inner_file_name.endswith(".latency-history.csv"):
                    continue

                csv_df = load_latency_history_csv(f"{outer_path}/{inner_file_name}", worker_id)
                result = inner_join(result, csv_df)

    return result


def load_latency_history_csv(file_path, worker_id):
    print(f"load_latency_history_csv {file_path}")
    test_id = os.path.basename(file_path).replace(".latency-history.csv", "")
    df = pd.read_csv(file_path, skiprows=2)

    for column_name in df.columns:
        if column_name.startswith("Unnamed"):
            df.drop([column_name], inplace=True, axis=1)

    df['time'] = df['StartTime'].round(0).astype(int)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    # print(csv_df)
    for column_name in df.columns:
        if column_name == "time":
            continue

        column_title = ColumnTitle("Latency", column_name,
                                   {"test_id": test_id, "worker_id": worker_id})
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
    for column_name in df.columns:
        column_title = ColumnTitle.from_string(column_name)
        if column_title.group != "Operations":
            continue

        if column_title.metric != "operations/second":
            continue

        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

        plt.plot(df.index, df[column_name], label=column_name)

        plt.ticklabel_format(style='plain', axis='y')
        plt.ylabel("Throughput (operations/second)")
        plt.xlabel("Time")
            # plt.gca.xaxis.set_major_formatter(mdates.DateFormatter('%m-%S'))
        plt.legend()
        #if worker_id == '':
        #    plt.title(f"{test_id} {column_name.capitalize()}")
        #else:

        test_id = column_title.attributes.get("test_id")
        if test_id is None:
            test_str = ""
        else:
            test_str = f"-{test_id}"

        worker_id = column_title.attributes.get("worker_id")

        plt.title(f"{column_name.capitalize()}")
        plt.grid()

        if worker_id is None:
            path = f"{report_dir}/throughput{test_str}.png"
        else:
            dir = f"{report_dir}/{worker_id}"
            if not os.path.exists(dir):
                os.mkdir(dir)
            path = f"{dir}/throughput{test_str}.png"

        print(f"Generating [{path}]")
        plt.savefig(path)
        plt.close()


benchmark_htable = Benchmark("htable", "/home/pveentjer/cpu_1_affinity_1")
print(benchmark_htable.run_count())
benchmark_htable.latest_run_path()

# benchmark_map = Benchmark("map", "/eng/Hazelcast/alto-testing/alto-new-testing/runs/htable/read_only/1KB/cpu_1/")
# print(benchmark_map.run_count())

report_dir = "/mnt/home/pveentjer/cpu_1_affinity_1/18-09-2023_10-10-22/report/"#tempfile.mkdtemp()
print(f"Report directory {report_dir}")

log("Loading performance data: Starting")
performance_data_runs = {}

run = benchmark_htable.latest_run_path()
print(f"Analyzing run_path:{run.path}")
# processing_hdr(run.path)
df_performance = load_operations_data(run.path)
df_latency_history = load_latency_history(run.path)
df = inner_join(df_performance, df_latency_history)

# df = df_latency_history

path_excel = f"{run.path}/data.xlsx"
print(f"path excel: {path_excel}")
df.to_excel(path_excel)

path_csv = f"{run.path}/data.csv"
print(f"path csv: {path_csv}")
df.to_csv(path_csv)
for column_name in df.columns:
    print(column_name)
plot_operations(report_dir, df)
plot_latency_history(report_dir, df)


#     # for (test_id, agent_id), df in data.items():
#     #    performance_data_runs[(test_id, agent_id, run.id)] = df
#
#     # for worker_id, df in worker_map.items():
#     #     epoch_column = df['epoch']
#     #     run.worker_periods[(worker_id, test_id)] = Period(epoch_column.iloc[0], epoch_column.iloc[-1])

log("Loading performance data: Done")
# aggregate_performance_data(performance_dfs_1)
# aggregate_performance_data(performance_dfs_2)
# log("Plotting performance data: Starting")
# plot_performance(report_dir, performance_data_runs)
# log("Plotting performance data: Done")

# log("Processing HDR: Starting")
# latency_history_data_runs = {}
# for run in run_list:
#     print(f"run {run}")
#     processing_hdr(run.path)
# log("Processing HDR: Done")
# log("Loading latency history: Starting")
# latency_history_data_runs = {}
# for run in run_list:
#     latency_history_data_runs[run.id] = load_latency_history(run.path)
# log("Loading latency history: Done")
# log("Plotting latency history: Starting")
# plot_latency_history(report_dir, latency_history_data_runs)
# log("Plotting latency history: Done")
#
# log("Loading dstat data: Starting")
# dstat_data_runs = {}
# for run in run_list:
#     data = load_dstat(run.path)
#     for agent_id, df in data.items():
#         if not absolute_time:
#             to_relative_time(df, "epoch")
#         dstat_data_runs[(agent_id, run.id)] = df
# log("Loading dstat data: Done")
# log("Plotting dstat data: Starting")
# plot_dstat(report_dir, dstat_data_runs)
# log("Plotting dstat data: Done")
#
# log("Loading latency-distribution data")
# # r1 = load_latency_distribution(
# #     "/home/eng/Hazelcast/simulator-tng/storage/runs/insert/1000M/map_tiered/02-08-2022_15-33-08/report/tmp/1")
# # r2 = load_latency_distribution(
# #     "/home/eng/Hazelcast/simulator-tng/storage/runs/insert/1000M/map_tiered/01-08-2022_08-57-42/report/tmp/1")
# #
#
# log("Loading latency-distribution data: Done")
# log("Plotting latency histogram data: Starting")
# # plot_cumulative_latency_distribution(report_dir, r1, r2)
# # plot_latency_histogram(report_dir, r1)
# log("Plotting latency histogram data: Done")
