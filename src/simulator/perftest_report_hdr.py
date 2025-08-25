#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from matplotlib.ticker import FuncFormatter
from pandas.errors import EmptyDataError

from simulator.log import warn
from simulator.perftest_report_common import *
from simulator.util import shell, simulator_home, read_file, write_file


def prepare_hdr(config: ReportConfig):
    for run_label, run_dir in config.runs.items():
        __merge_worker_hdr(run_dir)
        __process_hdr(config, run_dir, run_label)


def __merge_worker_hdr(run_dir):
    dic = {}
    for worker_dir_name in os.listdir(run_dir):
        worker_dir = f"{run_dir}/{worker_dir_name}"
        worker_id = extract_worker_id(worker_dir)
        if not worker_id:
            continue

        for file_name in os.listdir(worker_dir):
            if not file_name.endswith(".hdr"):
                continue

            hdr_file = f"{run_dir}/{worker_dir_name}/{file_name}"
            files = dic.get(file_name)
            if not files:
                files = []
                dic[file_name] = files
            files.append(hdr_file)


    for file_name, hdr_files in dic.items():
        # write the list of files into a file and let Java processor read from it
        hdr_list_file_name = file_name + "._merge_worker_hdr_input_files_list"
        hdr_list_file = os.path.join(run_dir, hdr_list_file_name)
        print(f"Writing file list for {file_name} into file {hdr_list_file}")
        with open(hdr_list_file, 'w') as listing_file:
            for hdr_file_path in hdr_files:
                listing_file.write(hdr_file_path + '\n')

        command = f"""java -cp "{simulator_home}/lib/*" \
                         com.hazelcast.simulator.utils.HistogramLogMerger \
                         {run_dir}/{file_name} {hdr_list_file} 2>/dev/null"""

        print(f"Executing process for {command}")
        shell(command)


def __process_hdr(config: ReportConfig, run_dir, run_label):
    log_sub_section("Processing hdr files: Start")
    start_sec = time.time()
    hdr_batch_process_details = os.path.join(run_dir, "hdr_batch_process_details")
    prepared_hdr_files = []

    with open(hdr_batch_process_details, 'w') as batch_process_output:
        for outer_file_name in os.listdir(run_dir):
            if outer_file_name.endswith(".hdr"):
                prepared_hdr_files.append(__prepare_hdr_file(
                    config, run_label, None, f"{run_dir}/{outer_file_name}", batch_process_output))
                continue

            worker_dir = f"{run_dir}/{outer_file_name}"
            worker_id = extract_worker_id(worker_dir)

            if not worker_id:
                continue

            for inner_file_name in os.listdir(worker_dir):
                if not inner_file_name.endswith(".hdr"):
                    continue
                hdr_file = f"{worker_dir}/{inner_file_name}"
                prepared_hdr_files.append(__prepare_hdr_file(
                    config, run_label, worker_id, hdr_file, batch_process_output))
    
    info(f"Processing {len(prepared_hdr_files)} hdr files")
    hdr_processing_cmd = f"""java -cp "{simulator_home}/lib/*" \
                    com.hazelcast.simulator.utils.ReportHistogramLogProcessor {hdr_batch_process_details}"""
    status = shell(hdr_processing_cmd)
    if status != 0:
        raise Exception(
            f"hdr processing failed with status {status}, cmd executed: \"{hdr_processing_cmd}\"")

    for hdr_output_dir, hdr_file_name_no_ext in prepared_hdr_files:
        os.rename(f"{hdr_output_dir}/{hdr_file_name_no_ext}",
                  f"{hdr_output_dir}/{hdr_file_name_no_ext}.latency-history.csv")
    os.remove(hdr_batch_process_details)

    duration_sec = time.time() - start_sec
    log_sub_section(f"Processing hdr files: Done {duration_sec:.2f} seconds")


def __prepare_hdr_file(config: ReportConfig, run_label, worker_id, hdr_file, batch_process_output):
    info(f"\t Preparing hdr file {hdr_file} for processing")

    hdr_file_name_no_ext = Path(hdr_file).stem

    if worker_id is None:
        target_dir = f"{config.report_dir}/hdr/{run_label}/"
    else:
        target_dir = f"{config.report_dir}/hdr/{run_label}/{worker_id}"
    mkdir(target_dir)

    # we need to apply the start/end so that the aggregated hdr dats for the whole run is correct.
    # otherwise it will contain the data during the warmup/cooldown and isn't correct.
    start_end = ""
    period = config.periods[run_label]
    if config.warmup_seconds > 0:
        start_end = f" -start {config.warmup_seconds} "

    if config.cooldown_seconds > 0:
        duration = period.end_time - period.start_time
        end = duration - config.cooldown_seconds
        start_end += f" -end {end} "
    
    batch_process_output.write(f"{start_end} -i {hdr_file} -o {target_dir}/{hdr_file_name_no_ext} -outputValueUnitRatio 1000\n")
    return target_dir, hdr_file_name_no_ext


def analyze_latency_history(report_dir, attributes):
    log_section("Loading latency history data: Start")
    run_label = attributes["run_label"]

    start_sec = time.time()

    # iterate over the files in the run directory
    dir = f"{report_dir}/hdr/{run_label}"

    if not os.path.exists(dir):
        warn(f"Skipping hdr latency analysis, dir [{dir}] does not exist.")
        return None

    all_latency_histories = []
    for outer_file_name in os.listdir(dir):
        outer_path = f"{dir}/{outer_file_name}"
        if outer_file_name.endswith(".latency-history.csv"):
            all_latency_histories.append(__load_latency_history_csv(outer_path, attributes, None))
        elif os.path.isdir(outer_path):
            worker_id = outer_file_name
            # iterate over the files in the worker directory
            for inner_file_name in os.listdir(outer_path):
                if not inner_file_name.endswith(".latency-history.csv"):
                    continue

                all_latency_histories.append(__load_latency_history_csv(
                    f"{outer_path}/{inner_file_name}", attributes, worker_id))

    result = concat_dataframe_columns(all_latency_histories)
    duration_sec = time.time() - start_sec
    log_section(f"Loading latency history data: Done (duration {duration_sec:.2f} seconds)")
    return result


def __load_latency_history_csv(file_path, attributes, worker_id):
    test_id = os.path.basename(file_path).replace(".latency-history.csv", "")
    info(f"\tLoading {file_path}")
    try:
        df = pd.read_csv(file_path, skiprows=2)
    except EmptyDataError:
        # in case of an empty csv
        return pd.DataFrame()

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


def report_hdr(config: ReportConfig, df):
    __report_latency_history(config, df)
    __report_hgrm(config)
    __make_report_csv(config)


def __make_report_csv(config: ReportConfig):
    for run_label, run_dir in config.runs.items():
        hdr_dir = f"{config.report_dir}/hdr/{run_label}"
        for file_name in sorted(os.listdir(hdr_dir)):
            if not file_name.endswith(".hgrm"):
                continue

            hdr_file_name_no_ext = Path(file_name).stem
            shell(f"""java -cp "{simulator_home}/lib/*" \
                           com.hazelcast.simulator.utils.ReportCsv \
                           {hdr_dir}/{hdr_file_name_no_ext}.hgrm \
                           {config.report_dir} \
                           {run_label}""")


def __report_latency_history(config: ReportConfig, df):
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
            target_dir = f"{config.report_dir}/latency/"
        elif not config.worker_reporting:
            continue
        else:
            target_dir = f"{config.report_dir}/latency/{worker_id}"
        mkdir(target_dir)

        if test_id is None:
            test_str = ""
        else:
            test_str = f"_{test_id}"

        filtered_df = None
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            if filtered_df is None:
                filtered_df = pd.DataFrame(index=df.index)

            filtered_df[run_label] = df[column_name].copy()

        filtered_df.dropna(inplace=True)
        filtered_df.to_csv(f"{target_dir}/{metric_id}{test_str}.csv")

        fig, ax = plt.subplots(figsize=(config.image_width_px / config.image_dpi,
                                        config.image_height_px / config.image_dpi),
                               dpi=config.image_dpi)
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            ax.plot(filtered_df.index, filtered_df[run_label], label=run_label)

        plt.ticklabel_format(style='plain', axis='y')
        plt.title(metric_id.replace('_', ' '))

        # trim wasted space on both sides of the plot
        plt.xlim(left=0, right=df.index[-1])

        if config.y_start_from_zero:
            plt.ylim(bottom=0)

        if metric_id == "Int_Throughput" or metric_id == "Total_Throughput":
            plt.ylabel("operations/second")
        elif metric_id == "Inc_Count" or metric_id == "Total_Count":
            plt.ylabel("operations")
        else:
            plt.ylabel("microseconds")

        if config.preserve_time:
            plt.xlabel("Time")
            ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d %H:%M:%S'))
        else:
            plt.xlabel("Time minutes:seconds")
            ax.xaxis.set_major_formatter(DateFormatter('%M:%S'))
        plt.legend()
        # if worker_id == '':
        #    plt.title(f"{test_id} {column_name.capitalize()}")
        # else:

        plt.grid()

        path = f"{target_dir}/{metric_id}{test_str}.png"
        info(f"\tGenerating [{path}]")
        plt.savefig(path)

        if config.svg:
            path = f"{target_dir}/{metric_id}{test_str}.svg"
            info(f"\tGenerating [{path}]")
            plt.savefig(path)

        # if config.interactive:
        #     path = f"{target_dir}/{metric_id}{test_str}.html"
        #     info(f"\tGenerating [{path}]")
        #     plotly_fig = tls.mpl_to_plotly(plt.gcf())
        #     plotly_fig.write_html(path)

        plt.close()

    duration_sec = time.time() - start_sec
    log_section(f"Plotting latency history: Done (duration {duration_sec:.2f} seconds)")


def __report_hgrm(config: ReportConfig):
    hgrm_files = set()
    for run_label, run_dir in config.runs.items():
        hgrm_files.update(__find_hgrm_files(config, run_label))

    __make_hgrm_latency_by_perc_dist_html(config, hgrm_files)
    # make_hgrm_histogram_plot(items)
    __make_latency_by_perc_dist_plot(config, hgrm_files)


def __find_hgrm_files(config: ReportConfig, run_label):
    result = []
    dir = f"{config.report_dir}/hdr/{run_label}"
    for outer_file_name in os.listdir(dir):
        if outer_file_name.endswith(".hgrm"):
            result.append(outer_file_name)
            continue

        dir_path = f"{dir}/{outer_file_name}"
        if not os.path.isdir(dir_path):
            continue

        if not config.worker_reporting:
            continue

        for inner_file_name in os.listdir(dir_path):
            if not inner_file_name.endswith(".hgrm"):
                continue
            result.append(f"{outer_file_name}/{inner_file_name}")
    return result


def __make_latency_by_perc_dist_plot(config: ReportConfig, hgrm_files):
    for hgrm_file_name in hgrm_files:
        plt.figure(figsize=(config.image_width_px / config.image_dpi,
                            config.image_height_px / config.image_dpi),
                   dpi=config.image_dpi)

        for run_label, run_path in config.runs.items():
            dir = f"{config.report_dir}/hdr/{run_label}"

            hgrm_file = f"{dir}/{hgrm_file_name}"
            if not os.path.exists(hgrm_file):
                continue

            df = __load_hgrm(hgrm_file)
            plt.plot(df['1/(1-Percentile)'], df['Value'], label=run_label)

        plt.xscale('log')

        xlabels = pd.DataFrame()
        xlabels['label'] = pd.Series(dtype='string')
        xlabels['tick'] = pd.Series(dtype='float')
        xlabels.loc[len(xlabels)] = ["0%", 1.0]
        xlabels.loc[len(xlabels)] = ["90%", 10.0]
        xlabels.loc[len(xlabels)] = ["99%", 100.0]
        xlabels.loc[len(xlabels)] = ["99.9%", 1_000.0]
        xlabels.loc[len(xlabels)] = ["99.99%", 10_000.0]
        xlabels.loc[len(xlabels)] = ["99.999%", 100_000.0]
        xlabels.loc[len(xlabels)] = ["99.9999%", 1_000_000.0]
        xlabels.loc[len(xlabels)] = ["99.99999%", 10_000_000.0]
        plt.xticks(xlabels['tick'], xlabels['label'])

        plt.ylabel("Latency")
        plt.xlabel("Percentile")
        plt.legend()
        plt.title(f"Latency distribution")
        plt.grid()
        plt.gca().yaxis.set_major_formatter(FuncFormatter(format_us_time_ticks))

        hgrm_file_path_no_ext = hgrm_file_name.rstrip(".hgrm")
        path = f"{config.report_dir}/latency/latency_distribution_{hgrm_file_path_no_ext}.png"
        mkdir(os.path.dirname(path))
        info(f"\tGenerating [{path}]")
        plt.savefig(path)

        if config.svg:
            path = f"{config.report_dir}/latency/latency_distribution_{hgrm_file_path_no_ext}.svg"
            info(f"\tGenerating [{path}]")
            plt.savefig(path)

        # if config.interactive:
        #     path = f"{config.report_dir}/latency/latency_distribution_{hgrm_file_path_no_ext}.html"
        #     info(f"\tGenerating [{path}]")
        #     plotly_fig = tls.mpl_to_plotly(plt.gcf())
        #     plotly_fig.write_html(path)

        plt.close()


def __make_hgrm_histogram_plot(config: ReportConfig, items):
    plt.figure(figsize=(config.image_width_px / config.image_dpi,
                        config.image_height_px / config.image_dpi),
               dpi=config.image_dpi)

    # df['Binned'] = pd.cut(df['Value'], bins=5)
    total_w = 0.8
    n = len(items)
    # x = np.arange(n)
    w = total_w / n

    i = 0
    for label, path in items.items():
        df = __load_hgrm(path)
        hist = plt.hist(df['Value'], bins=20, weights=df['Count'])
        # plt.histogram(df["Value"], df["Count"], label=label)

    plt.ticklabel_format(style='plain', axis='y')
    # plt.xticks(x + (total_w / n), df["Value"].index)
    plt.ylabel("count")
    plt.yscale("log")
    plt.xlabel("latency (us)")
    plt.legend()
    # if worker_id == '':
    #    plt.title(f"{test_id} {column_name.capitalize()}")
    # else:

    plt.title(f"latency distribution")
    plt.grid()

    path = f"{config.report_dir}/latency_histogram.png"
    info(f"\tGenerating [{path}]")
    plt.savefig(path)
    plt.close()


def __load_hgrm(file_path):
    df = pd.DataFrame()
    df['Value'] = pd.Series(dtype='float')
    df['Percentile'] = pd.Series(dtype='float')
    df['TotalCount'] = pd.Series(dtype='int')
    df['1/(1-Percentile)'] = pd.Series(dtype=int)
    df['Count'] = pd.Series(dtype='float')
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

            if len(items) == 4:
                flipped_percentile = float(items[3])
            else:
                flipped_percentile = float('inf')
            count = total_count - prev_total_count

            row = [value, percentile, total_count, flipped_percentile, count]
            df.loc[len(df)] = row
            prev_total_count = total_count
    return df


def __make_hgrm_latency_by_perc_dist_html(config: ReportConfig, hgrm_files):
    for hgrm_file_name in hgrm_files:
        labels = []
        histos = []
        for run_label, run_path in config.runs.items():
            dir = f"{config.report_dir}/hdr/{run_label}"
            hgrm_file_path = f"{dir}/{hgrm_file_name}"
            if not os.path.exists(hgrm_file_path):
                continue

            hgrm_text = read_file(hgrm_file_path)
            histos.append(hgrm_text.strip())
            labels.append(run_label)

        html_template = read_file(f"{simulator_home}/src/simulator/latency_by_percentile.html")
        html = html_template.replace("{HISTOS}", str(histos))
        html = html.replace("{NAMES}", str(labels))
        hgrm_file_name_no_ext = hgrm_file_name.rstrip(".hgrm")
        target_file_name = f"{config.report_dir}/latency/{hgrm_file_name_no_ext}.html"
        mkdir(os.path.dirname(target_file_name))
        write_file(target_file_name, html)
