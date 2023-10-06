#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time

from simulator.perftest_report_shared import *
import matplotlib.pyplot as plt


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


def report_dstat(config: ReportConfig, df: pd.DataFrame):
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
        target_dir = f"{config.report_dir}/dstat/{agent_id}"
        mkdir(target_dir)

        # the df but with nan removed.
        filtered_df = None
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            if filtered_df is None:
                filtered_df = pd.DataFrame(index=df.index)

            filtered_df[run_label] = df[column_name].copy()

        filtered_df.dropna(inplace=True)
        filtered_df.to_csv(f"{target_dir}/{nice_metric_name}.csv")

        plt.figure(figsize=(config.image_width_px / config.image_dpi,
                            config.image_height_px / config.image_dpi),
                   dpi=config.image_dpi)
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            plt.plot(filtered_df.index, filtered_df[run_label], label=run_label)

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
