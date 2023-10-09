#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import shutil
import time

import pandas as pd
from simulator.perftest_report_shared import *
import matplotlib.pyplot as plt


def prepare_operation(config: ReportConfig):
    for run_dir in config.runs.values():
        # deal with legacy 'performance csv files'
        __fix_operations_filenames(run_dir)
        __create_aggregated_operations_csv(run_dir)


def analyze_operations(run_dir, attributes):
    log_section("Loading operations data: Start")
    start_sec = time.time()

    df_list = []
    df_list.extend(__load_aggregated_operations_csv(run_dir, attributes))
    df_list.extend(__load_worker_operations_csv(run_dir, attributes))

    result = None
    for df in df_list:
        result = merge_dataframes(result, df)

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


def __create_aggregated_operations_csv(run_dir):
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

        plt.figure(figsize=(config.image_width_px / config.image_dpi,
                            config.image_height_px / config.image_dpi),
                   dpi=config.image_dpi)
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            plt.plot(filtered_df.index, filtered_df[run_label], label=run_label)

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
