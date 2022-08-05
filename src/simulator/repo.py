import os

import pandas as pd

pd.options.mode.chained_assignment = None
from matplotlib import pyplot as plt
import tempfile
from simulator.log import log
#from util import write
image_dpi = 96
image_width_px = 1600
image_height_px = 1200
import matplotlib.pyplot as plt, mpld3

# todo:
# - improved dstat title
# - hgrm proper unit for y-label
# - hgrm location for files
# - comparison
# - performance: the y values should be fully written
# - hgrm: rename to latency
# - hgrm: latency distribution
# - generate html report
# - absolute vs relative time
# - trimming

# done
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

def multiple_by_thousand(df, *column_names):
    for column_name in column_names:
        column = df[column_name]
        values = len(column.values)
        for i in range(values):
            column.iloc[i] = 1000 * column.iloc[i]


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


def load_performance(benchmark_dir):
    log("Loading performance data")

    result = {}
    for dir_name in os.listdir(benchmark_dir):
        worker_dir = f"{benchmark_dir}/{dir_name}"
        worker_id = find_worker_id(worker_dir)
        if not worker_id:
            continue
        csv_path = f"{worker_dir}/performance.csv"
        if not os.path.exists(csv_path):
            continue
        df = pd.read_csv(csv_path, skiprows=0)
        result[worker_id] = df

    log("Loading performance data: done")

    return result


def plot_performance(report_dir, df_workers):
    log("Plotting performance data")

    for worker_name, df in df_workers.items():
        result_dir = f"{report_dir}/performance/{worker_name}"
        os.makedirs(result_dir)

        for c in range(2, len(df.columns)):
            column_name = df.columns[c]
            fig = plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

            df['epoch'] = pd.to_datetime(df['epoch'], unit='s')

            plt.plot(df['epoch'], df[column_name])
            filename = column_name.replace("/", "_")

            plt.ylabel("Throughput (operations/second)")
            plt.xlabel("Time")
            plt.title(f"{worker_name} - {column_name.capitalize()}")
            plt.grid()
            plt.savefig(f'{result_dir}/{filename}.png')

            #html = mpld3.fig_to_html(fig)
            #with open(f'{result_dir}/{filename}.html', 'w') as f:
            #    return f.write(html)
            #print(html)
            #write(, html)
            plt.close()
    log("Plotting performance data: done")


def load_hgrm(benchmark_dir):
    log("Loading hgrm data")
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
            result[worker_name] = df
    log("Loading hgrm data: done")
    return result


def plot_hgrm(report_dir, df_workers):
    log("Plotting hgrm data")

    for worker_name, df in df_workers.items():
        result_dir = f"{report_dir}/latency/{worker_name}"
        os.makedirs(result_dir)

        for c in range(2, len(df.columns)):
            column_name = df.columns[c]
            if column_name.startswith("Unnamed"):
                continue

            plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

            df['StartTime'] = pd.to_datetime(df['StartTime'], unit='s')
            plt.plot(df['StartTime'], df[column_name])
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
    log("Plotting hgrm data: done")


def load_dstat(benchmark_dir):
    log("Loading dstat data")

    result = {}
    for csv_filename in os.listdir(benchmark_dir):
        if not csv_filename.endswith("_dstat.csv"):
            continue
        worker_name = csv_filename[:csv_filename.index("_")]
        csv_path = f"{benchmark_dir}/{csv_filename}"
        df = pd.read_csv(csv_path, skiprows=5)
        multiple_by_thousand(df, 'used', 'free', 'buf', 'cach')
        result[worker_name] = df

    log("Loading dstat data: done")

    return result


dstat_titles = {"1m": "1 Minute load average",
                "5m": "5 Minute load average",
                "15m": "15 minute load average",
                "cach": "Memory Usage: Cached",
                "buf": "Memory Usage: Buffered",
                "free": "Memory Usage: Free",
                "used": "Memory Usage: Used"}


def plot_dstat(report_dir, df_agents):
    log("Plotting dstat data")

    for agent_name, df in df_agents.items():
        result_dir = f"{report_dir}/dstat/{agent_name}"

        os.makedirs(result_dir)

        for c in range(2, len(df.columns)):
            column_name = df.columns[c]
            plt.figure(figsize=(image_width_px / image_dpi, image_height_px / image_dpi), dpi=image_dpi)

            df['epoch'] = pd.to_datetime(df['epoch'], unit='s')

            title = dstat_titles.get(column_name)
            if title:
                plt.title(f"{agent_name} - {title} ({column_name})")
            else:
                plt.title(f"{agent_name} - {column_name}")

            plt.plot(df['epoch'], df[column_name])
            filename = column_name.replace("/", "_")
            plt.xlabel("Time")
            plt.grid()
            plt.savefig(f'{result_dir}/{filename}.png')
            plt.close()

    log("Plotting dstat data: done")


benchmark_dir = '/home/eng/Hazelcast/simulator-tng/storage/runs/insert/10M/map_tiered/03-08-2022_10-52-46'

report_dir = tempfile.mkdtemp()
print(f"directory {report_dir}")

hgrm_dir = "/home/eng/Hazelcast/simulator-tng/storage/runs/insert/10M/map_tiered/03-08-2022_09-48-16/report/tmp/1"
hgrm_dfs = load_hgrm(hgrm_dir)
plot_hgrm(report_dir, hgrm_dfs)

performance_dfs = load_performance(benchmark_dir)
plot_performance(report_dir, performance_dfs)

dstat_dfs = load_dstat(benchmark_dir)
plot_dstat(report_dir, dstat_dfs)
