#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from simulator.log import info


def multiple_by(df, amount, *column_names):
    for column_name in column_names:
        df[column_name] = df[column_name] * amount


def mkdir(path):
    from pathlib import Path
    Path(path).mkdir(parents=True, exist_ok=True)


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
    long_label = False
    preserve_time = False
    y_start_from_zero = False
    svg = False

    # interactive = True

    def __init__(self, report_dir):
        self.report_dir = report_dir
        self.runs = {}
        self.periods = {}


class ColumnDesc:
    separator = "::"
    kv_separator = "=="

    def __init__(self, group, metric, attributes=None):
        if attributes is None:
            attributes = {}
        self.group = group
        self.metric_id = metric
        self.attributes = attributes

    def to_string(self):
        result = f"{self.group}{ColumnDesc.separator}{self.metric_id}"

        if self.attributes is not None:
            for key, value in self.attributes.items():
                if not value is None:
                    result = result + f"{ColumnDesc.separator}{key}{ColumnDesc.kv_separator}{value}"
        return result

    @staticmethod
    def from_string(column_name):
        args = column_name.split(ColumnDesc.separator)
        group = args[0]
        metric = args[1]
        attributes = {}
        for k in range(2, len(args)):
            pair = args[k].split(ColumnDesc.kv_separator)
            attributes[pair[0]] = pair[1]

        return ColumnDesc(group, metric, attributes)


def concat_dataframe_columns(dataframes):
    if len(dataframes) == 0:
        return None
    else:
        return pd.concat(dataframes, axis=1, join='outer')


# Shifts the vales in the time index to the beginning (epoch). This is needed
# to be able to compare benchmarks that have run at different times.
# Any data points that end up with a negative time, will be removed.
def df_shift_time(df: pd.DataFrame, amount_seconds=None):
    if df is None or len(df.index) == 0:
        return df

    if amount_seconds is None:
        amount_seconds = -df.index[0].timestamp()

    df = df.shift(periods=amount_seconds, freq='S')
    # filter out rows with a negative time.
    epoch = datetime(1970, 1, 1, 0, 0, 0)
    df = df[df.index >= epoch]

    return df


def df_trim_time(df: pd.DataFrame, start_time_seconds, end_time_seconds):
    if df is None:
        return df

    end = np.datetime64(datetime.fromtimestamp(end_time_seconds, timezone.utc))
    start = np.datetime64(datetime.fromtimestamp(start_time_seconds, timezone.utc))
    df = df[df.index >= start]
    df = df[df.index <= end]
    return df


def extract_worker_id(path):
    if not os.path.isdir(path):
        return None

    basename = os.path.basename(path)
    if not basename.startswith("A"):
        return None

    index = basename.index("-")
    return basename[:index]


def format_us_time_ticks(value_us, _):
    if value_us >= 1e6:
        unit = "s"
        value = value_us / 1e6
    elif value_us >= 1e3:
        unit = "ms"
        value = value_us / 1e3
    else:
        unit = "us"
        value = value_us
    return f"{value:.2f} {unit}"


class Period:
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time

    def start_millis(self):
        return int(round(float(self.start_time) * 1000))

    def end_millis(self):
        return int(round(float(self.end_time) * 1000))


def log_section(text):
    info("---------------------------------------------------")
    info(text)
    info("---------------------------------------------------")


def log_sub_section(text):
    info("-----------------------")
    info(text)
    info("-----------------------")
