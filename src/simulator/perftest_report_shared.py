#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd


def multiple_by(df, amount, *column_names):
    for column_name in column_names:
        df[column_name] = df[column_name] * amount


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


class ReportConfig:
    image_dpi = 96
    image_width_px = 1600
    image_height_px = 1200
    warmup_seconds = 0
    cooldown_seconds = 0
    worker_reporting = True
    compare_last = False

    def __init__(self, report_dir):
        self.report_dir = report_dir
        self.runs = {}


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
