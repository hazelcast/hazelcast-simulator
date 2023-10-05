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


def sizeof_fmt(x):
    if x < 0:
        return ""
    for x_unit in ['bytes', 'kB', 'MB', 'GB', 'TB']:
        if x < 1024.0:
            return "%3.1f %s" % (x, x_unit)
        x /= 1024.0


class ColumnDesc:
    seperator = "::"
    kv_seperator = "=="

    def __init__(self, group, metric, attributes=None):
        if attributes is None:
            attributes = {}
        self.group = group
        self.metric_id = metric
        self.attributes = attributes

    def to_string(self):
        result = f"{self.group}{ColumnDesc.seperator}{self.metric_id}"

        if self.attributes is not None:
            for key, value in self.attributes.items():
                if not value is None:
                    result = result + f"{ColumnDesc.seperator}{key}{ColumnDesc.kv_seperator}{value}"
        return result

    @staticmethod
    def from_string(column_name):
        args = column_name.split(ColumnDesc.seperator)
        group = args[0]
        metric = args[1]
        attributes = {}
        for k in range(2, len(args)):
            pair = args[k].split(ColumnDesc.kv_seperator)
            attributes[pair[0]] = pair[1]

        return ColumnDesc(group, metric, attributes)


def merge_dataframes(*dfs):
    result = None
    for df in dfs:
        if df is None:
            continue
        elif result is None:
            result = df
        else:
            result = pd.concat([result, df], axis=1, join="outer")
    return result


# Shifts the vales in the time index to the beginning (epoch). This is needed
# to be able to compare benchmarks that have run at different times.
def shift_to_epoch(df):
    if df is None or len(df.index) == 0:
        return df

    epoch_time = -df.index[0].timestamp()
    print(epoch_time)
    shifted_df = df.shift(periods=epoch_time, freq='S')
    return shifted_df


def extract_worker_id(path):
    if not os.path.isdir(path):
        return None

    basename = os.path.basename(path)
    if not basename.startswith("A"):
        return None

    index = basename.index("-")
    return basename[:index]


def log_section(text):
    print("---------------------------------------------------")
    print(text)
    print("---------------------------------------------------")


def log_sub_section(text):
    print("-----------------------")
    print(text)
    print("-----------------------")


def report_latency_history(report_dir, df):
    log_section("Plotting latency history: Start")
    start_sec = time.time()

    grouped_column_names = {}
    for column_name in df.columns:
        column_desc = ColumnDesc.from_string(column_name)
        if column_desc.group != "Latency":
            continue
        if column_desc.metric_id == "StartTime" or column_desc.metric_id == "Timestamp":
            continue
        metric_id = column_desc.metric_id
        worker_id = column_desc.attributes.get("worker_id")
        test_id = column_desc.attributes.get("test_id")
        column_names = grouped_column_names.get((worker_id, metric_id, test_id, worker_id))
        if column_names is None:
            column_names = []
            grouped_column_names[(worker_id, metric_id, test_id, worker_id)] = column_names
        column_names.append(column_name)

    for (worker_id, metric_id, test_id, worker_id), column_name_list in grouped_column_names.items():
        if worker_id is None:
            target_dir = f"{report_dir}/latency/"
        else:
            target_dir = f"{report_dir}/latency/{worker_id}"
        mkdir(target_dir)

        if test_id is None:
            test_str = ""
        else:
            test_str = f"-{test_id}"

        filtered_df = None
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_id = column_desc.attributes["run_id"]
            if filtered_df is None:
                filtered_df = pd.DataFrame(index=df.index)

            filtered_df[run_id] = df[column_name].copy()

        filtered_df.dropna(inplace=True)
        filtered_df.to_csv(f"{target_dir}/{metric_id}{test_str}.csv")

        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_id = column_desc.attributes["run_id"]
            plt.plot(filtered_df.index, filtered_df[run_id], label=run_id)

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

        plt.grid()

        path = f"{target_dir}/{metric_id}{test_str}.png"
        print(f"\tGenerating [{path}]")
        plt.savefig(path)
        plt.close()

    duration_sec = time.time() - start_sec
    log_section(f"Plotting latency history: Done (duration {duration_sec:.2f} seconds)")


def analyze_dstat(run_dir, attributes):
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
        # get rid of duplicates
        df = df.loc[~df.index.duplicated(keep='last')]
        df.drop(['epoch'], inplace=True, axis=1)

        new_attributes = attributes.copy()
        new_attributes["agent_id"] = agent_id
        for column_name in df.columns:
            if column_name == "time":
                continue

            column_desc = ColumnDesc("dstat", column_name, new_attributes)
            df.rename(columns={column_name: column_desc.to_string()}, inplace=True)

        result = merge_dataframes(result, df)

    duration_sec = time.time() - start_sec
    log_section(f"Loading dstat data: Done (duration {duration_sec:.2f} seconds)")
    return result


def report_dstat(report_dir, df):
    log_section("Plotting dstat data: Start")
    start_sec = time.time()

    grouped_column_names = {}
    for column_name in df.columns:
        column_desc = ColumnDesc.from_string(column_name)
        if column_desc.group != "dstat":
            continue
        metric_id = column_desc.metric_id
        agent_id = column_desc.attributes["agent_id"]
        column_names = grouped_column_names.get((agent_id, metric_id))
        if column_names is None:
            column_names = []
            grouped_column_names[(agent_id, metric_id)] = column_names
        column_names.append(column_name)

    for (agent_id, metric_id), column_name_list in grouped_column_names.items():
        nice_metric_name = metric_id.replace("/", "_").replace(":", "_")
        target_dir = f"{report_dir}/dstat/{agent_id}"
        mkdir(target_dir)

        filtered_df = None
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_id = column_desc.attributes["run_id"]
            if filtered_df is None:
                filtered_df = pd.DataFrame(index=df.index)

            filtered_df[run_id] = df[column_name].copy()

        filtered_df.dropna(inplace=True)
        filtered_df.to_csv(f"{target_dir}/{nice_metric_name}.csv")

        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_id = column_desc.attributes["run_id"]
            plt.plot(filtered_df.index, filtered_df[run_id], label=run_id)

        plt.ticklabel_format(style='plain', axis='y')
        plt.ylabel(metric_id)
        plt.xlabel("Time")
        # plt.gca.xaxis.set_major_formatter(mdates.DateFormatter('%m-%S'))
        plt.legend()

        plt.title(f"Agent {agent_id} : {metric_id}")
        plt.grid()

        path = f"{target_dir}/{nice_metric_name}.png"
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

warmup_seconds = 5
cooldown_seconds = 5


def analyze_operations_data(run_dir, attributes):
    log_section("Loading operations data: Start")
    start_sec = time.time()

    # deal with legacy 'performance csv files'
    fix_operations_filenames(run_dir)

    create_aggregated_operations_csv(run_dir)

    df_list = []
    df_list.extend(load_aggregated_operations_csv(run_dir, attributes))
    df_list.extend(load_worker_operations_csv(run_dir, attributes))

    result = None
    for df in df_list:
        result = merge_dataframes(result, df)

    duration_sec = time.time() - start_sec
    log_section(f"Loading operations data: Done (duration {duration_sec:.2f} seconds)")

    return result


def load_worker_operations_csv(run_dir, attributes):
    result = []
    # load the df of the workers.
    for outer_file in os.listdir(run_dir):
        worker_dir = f"{run_dir}/{outer_file}"
        worker_id = extract_worker_id(worker_dir)

        if not worker_id:
            continue

        for inner_file_name in os.listdir(worker_dir):
            if not inner_file_name.startswith("operations") or not inner_file_name.endswith(".csv"):
                continue

            if inner_file_name.startswith("operations"):
                test_id = inner_file_name.replace("operations-", "").replace(".csv", "")
            else:
                test_id = None

            csv_path = f"{worker_dir}/{inner_file_name}"
            print(f"\tLoading {csv_path}")
            df = pd.read_csv(csv_path)
            if len(df.index) == 0:
                continue

            # we need to round the epoch time to the nearest second
            df['time'] = df['epoch'].round(0).astype(int)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df.set_index('time', inplace=True)
            # get rid of duplicates
            df = df.loc[~df.index.duplicated(keep='last')]
            df.drop(['epoch'], inplace=True, axis=1)
            df.drop(['timestamp'], inplace=True, axis=1)

            new_attributes = attributes.copy()
            new_attributes["test_id"] = test_id
            new_attributes["worker_id"] = worker_id

            # print(new_attributes)
            for column_name in df.columns:
                # print(type(column_name))
                if column_name == "time":
                    continue
                column_desc = ColumnDesc("Operations", column_name, new_attributes)
                # print(column_name)
                # print(df[column_name])
                df.rename(columns={column_name: column_desc.to_string()}, inplace=True)
            result.append(df)

    return result


# Renames the names of old performance csv files
def fix_operations_filenames(run_dir):
    for outer_file_name in os.listdir(run_dir):
        outer_dir = f"{run_dir}/{outer_file_name}"
        worker_id = extract_worker_id(outer_dir)

        if not worker_id:
            continue

        for inner_file_name in os.listdir(outer_dir):
            if not inner_file_name.startswith("performance") or not inner_file_name.endswith(".csv"):
                continue
            if inner_file_name == "performance.csv":
                shutil.copyfile(f"{outer_dir}/{inner_file_name}",
                                f"{outer_dir}/operations.csv")
            elif inner_file_name.startswith("performance."):
                test_id = inner_file_name.replace("performance.", "").replace(".csv", "")
                shutil.copyfile(f"{outer_dir}/{inner_file_name}",
                                f"{outer_dir}/operations-{test_id}.csv")
            elif inner_file_name.startswith("performance-"):
                test_id = inner_file_name.replace("performance-", "").replace(".csv", "")
                shutil.copyfile(f"{outer_dir}/{inner_file_name}",
                                f"{outer_dir}/operations-{test_id}.csv")


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
        # get rid of duplicates
        df = df.loc[~df.index.duplicated(keep='last')]
        df.drop(['epoch'], inplace=True, axis=1)
        df.drop(['timestamp'], inplace=True, axis=1)

        new_attributes = attributes.copy()
        new_attributes["test_id"] = test_id
        for column_name in df.columns:
            if column_name == "time":
                continue
            column_desc = ColumnDesc("Operations", column_name, new_attributes)
            df.rename(columns={column_name: column_desc.to_string()}, inplace=True)

        result.append(df)
    return result


def create_aggregated_operations_csv(run_dir):
    df_list_map = {}

    # load all the operation datafromes for every worker/test
    for outer_file_name in os.listdir(run_dir):
        outer_dir = f"{run_dir}/{outer_file_name}"
        worker_id = extract_worker_id(outer_dir)
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
            if len(df.index) == 0:
                continue
            df['epoch'] = df['epoch'].round(0).astype(int)
            df.set_index('epoch', inplace=True)
            # get rid of duplicates
            df = df.loc[~df.index.duplicated(keep='last')]
            df_list.append(df)

    # merge the frames into the
    for test_id, df_list in df_list_map.items():
        aggr_df = df_list[0]
        # print("*********************************")
        # for column in aggr_df.columns:
        #     print(column)

        for df_index in range(1, len(df_list)):
            df = df_list[df_index]
            for row_ind in df.index:
                # print("----")
                # print(row_ind)
                row = df.T.get(row_ind)
                aggr_row = aggr_df.T.get(row_ind)
                if aggr_row is None:
                    # it is a row with a time that doesn't exist in the aggregate
                    # so the whole row can be added.
                    aggr_df.loc[row_ind] = row
                else:
                    # print("==========================")

                    # it is a row with a time that already exist. So a new row is
                    # created whereby every value from the 2 rows are added into
                    # a new row and that is written back to the aggr_df
                    new_aggr_row = []
                    for value_ind in range(len(row)):
                        # print("----------------------------")
                        value = row.values[value_ind]
                        aggr_value = aggr_row.values[value_ind]
                        new_aggr_value = value + aggr_value
                        # print(f"type.value={type(value)} type.aggr_value={type(aggr_value)}")
                        new_aggr_row.append(new_aggr_value)
                    # print(len(row))
                    # print(len(aggr_df.loc[row_ind]))
                    # print(len(new_aggr_row))
                    # if row_ind is None:
                    #    print("oh shit")
                    aggr_df.loc[row_ind] = new_aggr_row

        aggr_df.to_csv(f"{run_dir}/operations{test_id}.csv")


def analyze_latency_history(run_dir, attributes):
    log_section("Loading latency history data: Start")

    start_sec = time.time()
    result = None

    merge_worker_hdr(run_dir)
    process_hdr(run_dir)

    # iterate over the files in the run directory
    for outer_file_name in os.listdir(run_dir):
        outer_path = f"{run_dir}/{outer_file_name}"
        if outer_file_name.endswith(".latency-history.csv"):
            csv_df = load_latency_history_csv(outer_path, attributes, None)
            result = merge_dataframes(result, csv_df)
        else:
            worker_id = extract_worker_id(outer_path)

            if not worker_id:
                continue

            # iterate over the files in the worker directory
            for inner_file_name in os.listdir(outer_path):
                if not inner_file_name.endswith(".latency-history.csv"):
                    continue

                csv_df = load_latency_history_csv(
                    f"{outer_path}/{inner_file_name}", attributes, worker_id)
                result = merge_dataframes(result, csv_df)

    duration_sec = time.time() - start_sec
    log_section(f"Loading latency history data: Done (duration {duration_sec:.2f} seconds)")
    return result


def process_hdr(run_dir):
    log_sub_section("Processing hdr files: Start")
    start_sec = time.time()

    for outer_file_name in os.listdir(run_dir):
        if outer_file_name.endswith(".hdr"):
            process_hdr_file(f"{run_dir}/{outer_file_name}")
            continue

        worker_dir = f"{run_dir}/{outer_file_name}"
        worker_id = extract_worker_id(worker_dir)

        if not worker_id:
            continue

        for inner_file_name in os.listdir(worker_dir):
            if not inner_file_name.endswith(".hdr"):
                continue
            hdr_file = f"{worker_dir}/{inner_file_name}"
            process_hdr_file(hdr_file)

    duration_sec = time.time() - start_sec
    log_sub_section(f"Processing hdr files: Done {duration_sec:.2f} seconds)")
    pass


def process_hdr_file(hdr_file):
    print(f"\t processing hdr file {hdr_file}")

    hdr_file_name_no_ext = Path(hdr_file).stem
    hdr_file_dir = Path(hdr_file).parent
    if os.path.exists(f"{hdr_file_dir}/{hdr_file_name_no_ext}.latency-history.csv"):
        return

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


def merge_worker_hdr(run_dir):
    dic = {}
    for worker_dir_name in os.listdir(run_dir):
        dir_path = f"{run_dir}/{worker_dir_name}"
        worker_name = extract_worker_id(dir_path)
        if not worker_name:
            continue

        for file_name in os.listdir(dir_path):
            if not file_name.endswith(".hdr"):
                continue

            hdr_file = f"{run_dir}/{worker_dir_name}/{file_name}"
            files = dic.get(file_name)
            if not files:
                files = []
                dic[file_name] = files
            files.append(hdr_file)

    for file_name, hdr_files in dic.items():
        util.shell(f"""java -cp "{util.simulator_home}/lib/*" \
                         com.hazelcast.simulator.utils.HistogramLogMerger \
                         {run_dir}/{file_name} {" ".join(hdr_files)} 2>/dev/null""")


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
    # get rid of duplicates
    df = df.loc[~df.index.duplicated(keep='last')]

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
        column_desc = ColumnDesc("Latency", metric, new_attributes)
        df.rename(columns={column_name: column_desc.to_string()}, inplace=True)
    return df


@dataclass
class Period:
    start: int
    end: int


@dataclass
class Run:
    id: str
    dir: str
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

    def latest_run(self):
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


def report_operations(report_dir, df):
    log_section("Plotting operations data: Start")
    start_sec = time.time()

    grouped_column_names = {}
    for column_name in df.columns:
        column_desc = ColumnDesc.from_string(column_name)
        if column_desc.group != "Operations" or column_desc.metric_id != "operations/second":
            continue
        metric_id = column_desc.metric_id
        worker_id = column_desc.attributes.get("worker_id")
        test_id = column_desc.attributes.get("test_id")
        column_names = grouped_column_names.get((worker_id, metric_id, test_id, worker_id))
        if column_names is None:
            column_names = []
            grouped_column_names[(worker_id, metric_id, test_id, worker_id)] = column_names
        column_names.append(column_name)

    for (worker_id, metric_id, test_id, worker_id), column_name_list in grouped_column_names.items():
        if worker_id is None:
            target_dir = f"{report_dir}/operations"
        else:
            target_dir = f"{report_dir}/operations/{worker_id}"
        mkdir(target_dir)

        if test_id is None:
            test_str = ""
        else:
            test_str = f"-{test_id}"

        filtered_df = None
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_id = column_desc.attributes["run_id"]
            if filtered_df is None:
                filtered_df = pd.DataFrame(index=df.index)

            filtered_df[run_id] = df[column_name].copy()

        filtered_df.dropna(inplace=True)
        filtered_df.to_csv(f"{target_dir}/throughput{test_str}.csv")

        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_id = column_desc.attributes["run_id"]
            plt.plot(filtered_df.index, filtered_df[run_id], label=run_id)

        plt.ticklabel_format(style='plain', axis='y')
        plt.ylabel("operations/second")
        plt.xlabel("Time")

        plt.legend()
        # if worker_id == '':
        #    plt.title(f"{test_id} {column_name.capitalize()}")
        # else:

        plt.title(f"Throughput")
        plt.grid()

        path = f"{target_dir}/throughput{test_str}.png"
        print(f"\tGenerating [{path}]")
        plt.savefig(path)
        plt.close()

    duration_sec = time.time() - start_sec
    log_section(f"Plotting operations data: Done (duration {duration_sec:.2f} seconds)")


def analyze_run(run_dir, run_id=None):
    print(f"Analyzing run_path:{run_dir}")

    if run_id is None:
        run_id = os.path.basename(run_dir)

    attributes = {"run_id": run_id}

    result = None

    df_operations = analyze_operations_data(run_dir, attributes)
    result = merge_dataframes(result, df_operations)

    df_latency_history = analyze_latency_history(run_dir, attributes)
    result = merge_dataframes(result, df_latency_history)

    df_dstat = analyze_dstat(run_dir, attributes)
    result = merge_dataframes(result, df_dstat)

    print(f"Analyzing run_path:{run_dir}: Done")

    return result


def make_report(df):
    if df is None:
        return

    path_csv = f"{report_dir}/data.csv"
    print(f"path csv: {path_csv}")
    df.to_csv(path_csv)

    for column_name in df.columns:
        print(column_name)

    report_operations(report_dir, df)
    report_latency_history(report_dir, df)
    report_dstat(report_dir, df)


def report_hgrm(df):
    fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

    plt.plot(df["Percentile"].index, df["Value"])

    plt.ticklabel_format(style='plain', axis='y')
    plt.ylabel("latency (us)")
    plt.xlabel("percentile")
    plt.xscale('log')
    plt.legend()
    # if worker_id == '':
    #    plt.title(f"{test_id} {column_name.capitalize()}")
    # else:

    plt.title(f"latency distribution")
    plt.grid()

    path = f"{report_dir}/latency_distribution.png"
    print(f"\tGenerating [{path}]")
    plt.savefig(path)
    plt.close()

def report_hgrm2(df):
    fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

    #df['Binned'] = pd.cut(df['Value'], bins=5)

    plt.bar(df["Value"].index, df["Count"])


    plt.ticklabel_format(style='plain', axis='y')
    plt.ylabel("count")
    plt.xlabel("value")
    plt.legend()
    # if worker_id == '':
    #    plt.title(f"{test_id} {column_name.capitalize()}")
    # else:

    plt.title(f"latency distribution")
    plt.grid()

    path = f"{report_dir}/latency_distribution.png"
    print(f"\tGenerating [{path}]")
    plt.savefig(path)
    plt.close()

def load_hgrm(file_path):
    df = pd.DataFrame()
    df['Value'] = pd.Series(dtype='float')
    df['Percentile'] = pd.Series(dtype='float')
    df['TotalCount'] = pd.Series(dtype='int')
    df['Count'] = pd.Series(dtype='int')
    prev_total_count = 0
    with open(file_path) as f:
        lines = f.readlines()
        for line in lines[4:]:
            if line.startswith("#"):
                # at the end of the file
                break

            import re
            items = re.split(r'\s+', line.strip())

            value = float(items[0])
            percentile = float(items[1])
            total_count = int(items[2])
            count = total_count - prev_total_count
            row = [value, percentile, total_count, count]
            df.loc[len(df)] = row
            prev_total_count = total_count
    df.to_csv(f"{report_dir}/nonsense.csv")
    return df


start_sec = time.time()

report_dir = "/mnt/home/pveentjer/report/"  # tempfile.mkdtemp()
mkdir(report_dir)
print(f"Report directory {report_dir}")

# df_1 = analyze_run("/home/pveentjer/tmp/report/runs/valuelength_1000/04-10-2023_06-35-01","valuelength_1000")
# df_2 = analyze_run("/home/pveentjer/tmp/report/runs/valuelength_1/04-10-2023_08-00-07","valuelength_1")

df = None
# df = merge_dataframes(df, shift_to_epoch(df_1))
# df = merge_dataframes(df, shift_to_epoch(df_2))

# make_report(df)

df = load_hgrm("/home/pveentjer/tmp/report/runs/valuelength_1000/04-10-2023_06-35-01/map.get.hgrm")
report_hgrm2(df)

print(df)
duration_sec = time.time() - start_sec
log(f"Generating report: Done  (duration {duration_sec:.2f} seconds)")
