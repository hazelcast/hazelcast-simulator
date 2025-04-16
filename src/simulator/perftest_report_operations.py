#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import shutil
import time

from matplotlib.dates import DateFormatter

from simulator.perftest_report_common import *
from simulator.util import shell, simulator_home
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.offline as pyo
import plotly.tools as tls


def prepare_operation(config: ReportConfig):
    for run_dir in config.runs.values():
        __fix_operations_filenames(run_dir)
        __create_aggregated_operations_csv(run_dir)


def analyze_operations(run_dir, attributes):
    log_section("Loading operations data: Start")
    start_sec = time.time()

    df_list = []
    df_list += __load_aggregated_operations_csv(run_dir, attributes)
    df_list += __load_worker_operations_csv(run_dir, attributes)

    result = concat_dataframe_columns(df_list)

    duration_sec = time.time() - start_sec
    log_section(f"Loading operations data: Done (duration {duration_sec:.2f} seconds)")

    return result


def __load_worker_operations_csv(run_dir, attributes):
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
            info(f"\tLoading {csv_path}")
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

            for column_name in df.columns:
                if column_name == "time":
                    continue
                column_desc = ColumnDesc("Operations", column_name, new_attributes)
                df.rename(columns={column_name: column_desc.to_string()}, inplace=True)
            result.append(df)

    return result


# Renames the names of old performance csv files
def __fix_operations_filenames(run_dir):
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


def __load_aggregated_operations_csv(run_dir, attributes):
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
        info(f"\tLoading {csv_path}")
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


def __create_aggregated_operations_csv(run_dir):
    info("\tMerging worker operations")
    start_time = time.time()
    cmd = f"""java -cp "{simulator_home}/lib/*" \
                               com.hazelcast.simulator.utils.OperationsFileAggregator {run_dir}"""
    status = shell(cmd)
    if status != 0:
        raise Exception(f"Merge failed with status {status}, cmd executed: \"{cmd}\"")
    end_time = time.time()
    duration_seconds = end_time - start_time
    info(f"\tFinished merging worker operations in {duration_seconds:.1f} seconds.")


def report_operations(config: ReportConfig, df: pd.DataFrame):
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
            target_dir = f"{config.report_dir}/operations"
        elif not config.worker_reporting:
            continue
        else:
            target_dir = f"{config.report_dir}/operations/{worker_id}"
        mkdir(target_dir)

        if test_id is None:
            test_str = ""
        else:
            test_str = f"-{test_id}"

        filtered_df = None
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            if filtered_df is None:
                filtered_df = pd.DataFrame(index=df.index)

            filtered_df[run_label] = df[column_name].copy()

        filtered_df.dropna(inplace=True)
        filtered_df.to_csv(f"{target_dir}/throughput{test_str}.csv")

        fig, ax = plt.subplots(figsize=(config.image_width_px / config.image_dpi,
                                        config.image_height_px / config.image_dpi),
                               dpi=config.image_dpi)

        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            ax.plot(filtered_df.index, filtered_df[run_label], label=run_label)

        plt.ticklabel_format(style='plain', axis='y')
        plt.ylabel("operations/second")

        # trim wasted space on both sides of the plot
        plt.xlim(left=0, right=df.index[-1])

        if config.y_start_from_zero:
            plt.ylim(bottom = 0)

        if config.preserve_time:
            plt.xlabel("Time")
            ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M:%S'))
        else:
            plt.xlabel("Time minutes:seconds")
            ax.xaxis.set_major_formatter(DateFormatter('%M:%S'))

        plt.legend()
        plt.title(f"Throughput")
        plt.grid()

        path = f"{target_dir}/throughput{test_str}.png"
        info(f"\tGenerating [{path}]")
        plt.savefig(path)

        if config.svg:
            path = f"{target_dir}/throughput{test_str}.svg"
            info(f"\tGenerating [{path}]")
            plt.savefig(path)

        # if config.interactive:
        #     path = f"{target_dir}/throughput{test_str}.html"
        #     info(f"\tGenerating [{path}]")
        #     plotly_fig = tls.mpl_to_plotly(plt.gcf())
        #     plotly_fig.write_html(path)

        plt.close()

    duration_sec = time.time() - start_sec
    log_section(f"Plotting operations data: Done (duration {duration_sec:.2f} seconds)")
