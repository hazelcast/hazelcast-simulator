import os
from dataclasses import dataclass

import matplotlib
from matplotlib import mlab
import numpy as np
import pandas as pd
from matplotlib.ticker import StrMethodFormatter

pd.options.mode.chained_assignment = None
from matplotlib import pyplot as plt
import tempfile
from simulator.log import log
from typing import Dict

# from util import write
image_dpi = 96
image_width_px = 1600
image_height_px = 1200
import matplotlib.pyplot as plt, mpld3


# todo:
# - improved dstat title
# - hgrm proper unit for y-label
# - hgrm location for files
# - generate html report
# - trimming
# - perf: multiplot labels
# - dstat: multiplot labels
# - hgrm: multiplot labels
# - performance: y values comma seperated
# - latency global vs local latency files
# - test specific files
# - performance: aggregated performance is missing
# - latency distribution make use of run_list
# - instead of modifying the epoch column, we add a new column
# - performance epoch column is not formatted anymore as string


# done
# - hgrm start time to epoch
# - performance epoch column is now an int
# - performance epoch column not rounded. Still dealing with ms.
# - extract the test times per worker.
# - logging cleanup
# - performance data making use of run_list
# - dstat making use of run_list
# - latency making use of run_list
# - variable for the absolute time.
# - comparison
# - hgrm: latency distribution
# performance: the y values should be fully written
# - performance: plot multiple data series
# - hgrm: plot multiple data series
# - dstat: multiple data series plot add labels
# - dstat: epoch column fix in load
# - hgrm: epoch column fix in load
# - performance: epoch column fix in load
# - dstat: plot multiple data series
# - absolute vs relative time
# - move relative time to loading
# - hgrm: relative time option
# - throughput: relative time option
# - dstat: relative time option
# - hgrm: latency time on x label
# - hgrm: total standard deviation is not a valid number
# - hgrm: total throughput column is missing
# - hgrm: title 'Latency (microseconds)'
# - performance: title should include worker name
# - hgrm: title should include worker name
# - performance: capitalize title
# - dstat y-label: time
# - hgrm y-label: time
# - performance: fix y label
# - performance: fix x label
# - dstat include agent name
# - hgrm removed Unnamed
# - hgrm title name remove _
# - improved hgrm title
# - improved operations title
# - create worker directory with dstat data
# - pulled out image quality parameters
# - loading performance data
# - plotting performance data
# - grid
# - improved logging

@dataclass
class Period:
    start: int
    end: int


@dataclass
class Test:
    name: str
    path: str
    worker_periods: Dict[str, Period]


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


def find_worker_id(path):
    if not os.path.isdir(path):
        return None

    workername = os.path.basename(path)
    if not workername.startswith("A"):
        return None

    index = workername.index("-")
    return workername[:index]


def load_performance_data(benchmark_dir, absolute_time):
    data_per_worker = {}
    for dir_name in os.listdir(benchmark_dir):
        worker_dir = f"{benchmark_dir}/{dir_name}"
        worker_id = find_worker_id(worker_dir)
        if not worker_id:
            continue
        csv_path = f"{worker_dir}/performance.csv"
        if not os.path.exists(csv_path):
            continue
        df = pd.read_csv(csv_path)

        # not using this column
        df.drop(['timestamp'], inplace=True, axis=1)

        # we need to round the epoch time to the nearest second
        df['epoch'] = df['epoch'].round(0).astype(int)
         #if not absolute_time:
        #    to_relative_time(df, "epoch")
        #df['epoch'] = pd.to_datetime(df['epoch'], unit='s')
        data_per_worker[worker_id] = df
    return data_per_worker


def aggregate_performance_data(perf_data):
    log("Aggregating performance data")

    delta_map = {}
    epochs_data = []
    for df in perf_data.values():
        epoch_column = df['epoch']
        delta_column = df['operations-delta']
        for i in range(len(epoch_column.values)):
            epoch = epoch_column.iloc[i]

            if epoch in delta_map:
                delta_map[epoch] = delta_column.iloc[i] + delta_map[epoch]
            else:
                epochs_data.append(epoch)
                delta_map[epoch] = delta_column.iloc[i]

    operations_delta_data = []
    operations_data = []
    last_operations = None
    last_epoch = None
    epochs_data.sort()
    for epoch in epochs_data:
        if not last_epoch:
            last_epoch = epoch
            last_operations = 0

        delta = delta_map[epoch]
        operations_delta_data.append(delta)
        last_operations += delta
        operations_data.append(last_operations)

    df = pd.DataFrame({'epoch': epochs_data,
                       'operations': operations_data,
                       'operations-delta': operations_delta_data,
                       'operations/second': operations_delta_data})
    print(df)
    perf_data[''] = df
    log("Aggregating performance data: done")


def plot_performance(report_dir, performance_data_runs):
    df_workers_list = list(performance_data_runs.values())

    first_df_workers = df_workers_list[0]
    for worker_name, first_df in first_df_workers.items():
        result_dir = f"{report_dir}/performance/{worker_name}"
        if worker_name != '':
            os.makedirs(result_dir)

        for c in range(2, len(first_df.columns)):
            column_name = first_df.columns[c]
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

            plt.ticklabel_format(style='plain', axis='y')
            plt.ylabel("Throughput (operations/second)")
            plt.xlabel("Time")
            if worker_name == '':
                plt.title(f"{column_name.capitalize()}")
            else:
                plt.title(f"{worker_name} - {column_name.capitalize()}")
            plt.grid()
            filename = column_name.replace("/", "_")
            plt.savefig(f'{result_dir}/{filename}.png')
            plt.close()


def load_hgrm(benchmark_dir):
    result = {}
    for dir_name in os.listdir(benchmark_dir):
        dir_path = f"{benchmark_dir}/{dir_name}"
        worker_name = find_worker_id(dir_path)
        if not worker_name:
            continue

        for sub_dir_name in os.listdir(dir_path):
            if not sub_dir_name.endswith(".hgrm"):
                continue
            filename = os.path.splitext(sub_dir_name)[0]
            csv_path = f"{benchmark_dir}/{dir_name}/{filename}"
            df = pd.read_csv(csv_path, skiprows=2)
            df.rename(columns={'StartTime': 'epoch'}, inplace=True)
            print(f"{df['epoch'].dtype}")

            # if not absolute_time:
            #     to_relative_time(df, "StartTime")
            # df['StartTime'] = pd.to_datetime(df['StartTime'], unit='s')
            result[worker_name] = df
    return result


def plot_hgrm(report_dir, hgrm_data_runs):
    df_workers_list = list(hgrm_data_runs.values())

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


def load_dstat(benchmark_dir):
    result = {}
    for csv_filename in os.listdir(benchmark_dir):
        if not csv_filename.endswith("_dstat.csv"):
            continue
        agent_name = csv_filename[:csv_filename.index("_")]
        csv_path = f"{benchmark_dir}/{csv_filename}"
        df = pd.read_csv(csv_path, skiprows=5)
        multiple_by(df, 1000, 'used', 'free', 'buf', 'cach')
        # if not absolute_time:
        #     to_relative_time(df, "epoch")
        # df['epoch'] = pd.to_datetime(df['epoch'], unit='s')
        result[agent_name] = df
    return result


dstat_titles = {"1m": "1 Minute load average",
                "5m": "5 Minute load average",
                "15m": "15 minute load average",
                "cach": "Memory Usage: Cached",
                "buf": "Memory Usage: Buffered",
                "free": "Memory Usage: Free",
                "used": "Memory Usage: Used"}


def plot_dstat(report_dir, data_runs):
    df_agents_list = list(data_runs.values())

    first_df_agents = df_agents_list[0]
    for agent_name, first_df in first_df_agents.items():
        result_dir = f"{report_dir}/dstat/{agent_name}"

        os.makedirs(result_dir)

        for c in range(2, len(first_df.columns)):
            column_name = first_df.columns[c]
            fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

            title = dstat_titles.get(column_name)
            if title:
                plt.title(f"{agent_name} - {title} ({column_name})")
            else:
                plt.title(f"{agent_name} - {column_name}")

            plt.plot(first_df['epoch'], first_df[column_name])

            for i in range(1, len(df_agents_list)):
                other_df_agents = df_agents_list[i]
                if not agent_name in other_df_agents:
                    continue

                other_df = other_df_agents[agent_name]
                if not column_name in other_df:
                    continue
                plt.plot(other_df['epoch'], other_df[column_name])

            filename = column_name.replace("/", "_")
            plt.xlabel("Time")
            plt.grid()
            plt.savefig(f'{result_dir}/{filename}.png')
            plt.close()


def load_latency_distribution(benchmark_dir, warmup_second=0, cooldown_seconds=0):
    result = {}
    for dir_name in os.listdir(benchmark_dir):
        worker_dir = f"{benchmark_dir}/{dir_name}"
        worker_id = find_worker_id(worker_dir)
        if not worker_id:
            continue

        for file_name in os.listdir(worker_dir):
            if not file_name.endswith(".hgrm"):
                continue
            csv_path = f"{worker_dir}/{file_name}"
            print(csv_path)
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
            print(df.to_string())
            result[worker_id] = df
    return result


def plot_cumulative_latency_distribution(report_dir, *df_workers_list):
    log("Plotting cumulative latency distribution data")

    first_df_workers = df_workers_list[0]
    for worker_name, first_df in first_df_workers.items():
        result_dir = f"{report_dir}/latency/{worker_name}"
        os.makedirs(result_dir, exist_ok=True)

        fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
        # plt.yscale('log')
        plt.xscale('log')
        axes = plt.gca()

        axes.set_xticks([1, 10, 100, 1000, 10000, 100000, 1000000, 10000000])
        axes.set_xticklabels(["0%", "90%", "99%", "99.9%", "99.99%", "99.999%", "99.9999%", "99.99999%"])
        plt.plot(first_df['1/(1-Percentile)'], first_df['Value'])
        for i in range(1, len(df_workers_list)):
            other_df_worker = df_workers_list[i]
            if not worker_name in other_df_worker:
                continue

            other_df = other_df_worker[worker_name]
            plt.plot(other_df['1/(1-Percentile)'], other_df['Value'])

        plt.grid()
        plt.savefig(f'{result_dir}/cumulative_latency_distribution.png')
        plt.close()

    log("Plotting cumulative latency distribution data: done")


def plot_latency_histogram(report_dir, *df_workers_list):
    first_df_workers = df_workers_list[0]
    for worker_name, first_df in first_df_workers.items():
        result_dir = f"{report_dir}/latency/{worker_name}"

        os.makedirs(result_dir, exist_ok=True)
        fig = plt.figure(figsize=(2 * image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)
        first_df.plot(kind='bar', x='Value', y="Count", ax=plt.gca())
        for i in range(1, len(df_workers_list)):
            other_df_worker = df_workers_list[i]
            if not worker_name in other_df_worker:
                continue

            other_df = other_df_worker[worker_name]
            other_df.plot(kind='bar', x='Value', y="Count", ax=plt.gca())
        plt.xlabel("Latency (microseconds)")
        plt.ylabel("Count")
        plt.yscale('log')

        plt.grid()
        plt.savefig(f'{result_dir}/latency_histogram.png')
        plt.close()


absolute_time = False
warmup_seconds = 5
cooldown_seconds = 5

test_list = []
test_list.append(Test("map_tiered#1",
                      "/home/eng/Hazelcast/simulator-tng/storage/runs/insert/10M/map_tiered/05-08-2022_11-34-57",
                      {}))
test_list.append(Test("map_tiered#2",
                      "/home/eng/Hazelcast/simulator-tng/storage/runs/insert/10M/map_tiered/05-08-2022_11-44-30",
                      {}))

report_dir = tempfile.mkdtemp()
print(f"directory {report_dir}")

log("Loading performance data")
performance_data_runs = {}
for test in test_list:
    data = load_performance_data(test.path, absolute_time)
    for worker_id, df in data.items():
        epoch_column = df['epoch']
        test.worker_periods[worker_id] = Period(epoch_column.iloc[0], epoch_column.iloc[-1])
    performance_data_runs[test.name] = data
log("Loading performance data: Done")
# aggregate_performance_data(performance_dfs_1)
# aggregate_performance_data(performance_dfs_2)
log("Plotting performance data")
plot_performance(report_dir, performance_data_runs)
log("Plotting performance data: Done")

log("Loading hgrm data")
hgrm_data_runs = {}
for test in test_list:
    hgrm_data_runs[test.name] = load_hgrm(test.path)
log("Loading hgrm data: Done")
log("Plotting hgrm data")
plot_hgrm(report_dir, hgrm_data_runs)
log("Plotting hgrm data: Done")

log("Loading dstat data")
dstat_data_runs = {}
for test in test_list:
    dstat_data_runs[test.name] = load_dstat(test.path)
log("Loading dstat data: Done")
log("Plotting dstat data")
plot_dstat(report_dir, dstat_data_runs)
log("Plotting dstat data: Done")

log("Loading latency-distribution data")
# r1 = load_latency_distribution(
#     "/home/eng/Hazelcast/simulator-tng/storage/runs/insert/1000M/map_tiered/02-08-2022_15-33-08/report/tmp/1")
# r2 = load_latency_distribution(
#     "/home/eng/Hazelcast/simulator-tng/storage/runs/insert/1000M/map_tiered/01-08-2022_08-57-42/report/tmp/1")
#

log("Loading latency-distribution data: Done")
log("Plotting latency histogram data")
# plot_cumulative_latency_distribution(report_dir, r1, r2)
# plot_latency_histogram(report_dir, r1)
log("Plotting latency histogram data: Done")
