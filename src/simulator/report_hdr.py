import time
from pathlib import Path

from report_shared import *
import matplotlib.pyplot as plt
import util


def prepare_hdr(report_config: ReportConfig):
    for run_label, run_dir in report_config.runs.items():
        __merge_worker_hdr(run_dir)
        __process_hdr(report_config.report_dir, run_dir, run_label)



def __process_hdr(report_dir, run_dir, run_label):
    log_sub_section("Processing hdr files: Start")
    start_sec = time.time()

    for outer_file_name in os.listdir(run_dir):
        if outer_file_name.endswith(".hdr"):
            __process_hdr_file(report_dir, run_label, None, f"{run_dir}/{outer_file_name}")

            continue

        worker_dir = f"{run_dir}/{outer_file_name}"
        worker_id = extract_worker_id(worker_dir)

        if not worker_id:
            continue

        for inner_file_name in os.listdir(worker_dir):
            if not inner_file_name.endswith(".hdr"):
                continue
            hdr_file = f"{worker_dir}/{inner_file_name}"
            __process_hdr_file(report_dir, run_label, worker_id, hdr_file)

    duration_sec = time.time() - start_sec
    log_sub_section(f"Processing hdr files: Done {duration_sec:.2f} seconds)")




def __process_hdr_file(report_dir, run_label, worker_id, hdr_file):
    print(f"\t processing hdr file {hdr_file}")

    hdr_file_name_no_ext = Path(hdr_file).stem

    if worker_id is None:
        target_dir = f"{report_dir}/hdr/{run_label}/"
    else:
        target_dir = f"{report_dir}/hdr/{run_label}/{worker_id}"
    mkdir(target_dir)

    util.shell(f"""java -cp "{util.simulator_home}/lib/*" \
                          com.hazelcast.simulator.utils.SimulatorHistogramLogProcessor \
                          -i {hdr_file} \
                          -o {target_dir}/{hdr_file_name_no_ext} \
                          -outputValueUnitRatio 1000""")
    os.rename(f"{target_dir}/{hdr_file_name_no_ext}.hgrm",
              f"{target_dir}/{hdr_file_name_no_ext}.hgrm.bak")
    util.shell(f"""java -cp "{util.simulator_home}/lib/*" \
                             com.hazelcast.simulator.utils.SimulatorHistogramLogProcessor \
                             -csv \
                             -i {hdr_file} \
                             -o {target_dir}/{hdr_file_name_no_ext} \
                             -outputValueUnitRatio 1000""")
    os.rename(f"{target_dir}/{hdr_file_name_no_ext}.hgrm.bak",
              f"{target_dir}/{hdr_file_name_no_ext}.hgrm")
    os.rename(f"{target_dir}/{hdr_file_name_no_ext}",
              f"{target_dir}/{hdr_file_name_no_ext}.latency-history.csv")




def analyze_latency_history(report_dir, run_dir, attributes):
    log_section("Loading latency history data: Start")
    run_label = attributes["run_label"]

    start_sec = time.time()
    result = None


    # iterate over the files in the run directory
    dir = f"{report_dir}/hdr/{run_label}"
    for outer_file_name in os.listdir(dir):
        outer_path = f"{dir}/{outer_file_name}"
        if outer_file_name.endswith(".latency-history.csv"):
            csv_df = __load_latency_history_csv(outer_path, attributes, None)
            result = merge_dataframes(result, csv_df)
        elif os.path.isdir(outer_path):
            worker_id = outer_file_name
            # iterate over the files in the worker directory
            for inner_file_name in os.listdir(outer_path):
                if not inner_file_name.endswith(".latency-history.csv"):
                    continue

                csv_df = __load_latency_history_csv(
                    f"{outer_path}/{inner_file_name}", attributes, worker_id)
                result = merge_dataframes(result, csv_df)

    duration_sec = time.time() - start_sec
    log_section(f"Loading latency history data: Done (duration {duration_sec:.2f} seconds)")
    return result


def __load_latency_history_csv(file_path, attributes, worker_id):
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


def __merge_worker_hdr(run_dir):
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


def report_hdr(config: ReportConfig, df):
    __report_latency_history(config, df)
    __report_hgrm(config)
    __make_report_csv(config)


def __make_report_csv(config:ReportConfig):
    for run_label, run_dir in config.runs.items():
        hdr_dir = f"{config.report_dir}/hdr/{run_label}"
        for file_name in os.listdir(hdr_dir):
            if not file_name.endswith(".hgrm"):
                continue

            hdr_file_name_no_ext = Path(file_name).stem
            util.shell(f"""java -cp "{util.simulator_home}/lib/*" \
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
        else:
            target_dir = f"{config.report_dir}/latency/{worker_id}"
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
        filtered_df.to_csv(f"{target_dir}/{metric_id}{test_str}.csv")

        plt.figure(figsize=(config.image_width_px / config.image_dpi,
                            config.image_height_px / config.image_dpi),
                         dpi=config.image_dpi)
        for column_name in column_name_list:
            column_desc = ColumnDesc.from_string(column_name)
            run_label = column_desc.attributes["run_label"]
            plt.plot(filtered_df.index, filtered_df[run_label], label=run_label)

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




def __report_hgrm(report_config: ReportConfig):
    hgrm_files = set()
    for run_label, run_dir in report_config.runs.items():
        hgrm_files.update(__find_hgrm_files(report_config.report_dir, run_label))

    __make_hgrm_latency_by_perc_dist_html(report_config, hgrm_files)
    # make_hgrm_histogram_plot(items)
    __make_latency_by_perc_dist_plot(report_config, hgrm_files)


def __find_hgrm_files(report_dir, run_label):
    result = []
    dir = f"{report_dir}/hdr/{run_label}"
    for outer_file_name in os.listdir(dir):
        if outer_file_name.endswith(".hgrm"):
            result.append(outer_file_name)
            continue

        dir_path = f"{dir}/{outer_file_name}"
        if not os.path.isdir(dir_path):
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

        plt.ylabel("latency (us)")
        plt.xlabel("percentile")
        plt.legend()
        plt.title(f"latency distribution")
        plt.grid()

        hgrm_file_path_no_ext = hgrm_file_name.rstrip(".hgrm")
        path = f"{config.report_dir}/latency/{hgrm_file_path_no_ext}.png"
        mkdir(os.path.dirname(path))
        print(f"\tGenerating [{path}]")
        plt.savefig(path)
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
        print(df)
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
    print(f"\tGenerating [{path}]")
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

            # todo: better name for x. 1/(1-Percentile)
            if len(items) == 4:
                x = float(items[3])
            else:
                x = float('inf')
            count = total_count - prev_total_count

            row = [value, percentile, total_count, x, count]
            df.loc[len(df)] = row
            prev_total_count = total_count
    return df


def __make_hgrm_latency_by_perc_dist_html(report_config: ReportConfig, hgrm_files):
    for hgrm_file_name in hgrm_files:
        labels = []
        histos = []
        for run_label, run_path in report_config.runs.items():
            dir = f"{report_config.report_dir}/hdr/{run_label}"
            hgrm_file_path = f"{dir}/{hgrm_file_name}"
            if not os.path.exists(hgrm_file_path):
                continue

            hgrm_text = util.read(hgrm_file_path)
            histos.append(hgrm_text.strip())
            labels.append(run_label)

        html_template = util.read(f"{util.simulator_home}/src/simulator/plotFiles.html")
        html = html_template.replace("{HISTOS}", str(histos))
        html = html.replace("{NAMES}", str(labels))
        hgrm_file_name_no_ext = hgrm_file_name.rstrip(".hgrm")
        target_file_name = f"{report_config.report_dir}/latency/{hgrm_file_name_no_ext}.html"
        mkdir(os.path.dirname(target_file_name))
        util.write(target_file_name, html)
