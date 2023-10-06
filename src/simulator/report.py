from dataclasses import dataclass
import pandas as pd

pd.options.mode.chained_assignment = None
from simulator.log import log

from report_operations import *
from report_dstat import *
from report_hdr import *

warmup_seconds = 5
cooldown_seconds = 5


@dataclass
class Period:
    start: int
    end: int


def prepare(report_config: ReportConfig):
    shutil.rmtree(report_config.report_dir)
    mkdir(report_config.report_dir)
    prepare_operation(report_config)
    prepare_hdr(report_config)


def analyze(report_config: ReportConfig):
    df = None

    for run_label, run_dir in report_config.runs.items():
        if len(runs) == 1:
            df = analyze_run(report_config.report_dir, run_dir, run_label)
        else:
            tmp_df = analyze_run(report_config.report_dir, run_dir, run_label)
            df = merge_dataframes(df, shift_to_epoch(tmp_df))
    return df


def analyze_run(report_dir, run_dir, run_label=None):
    print(f"Analyzing run_path:{run_dir}")

    if run_label is None:
        run_label = os.path.basename(run_dir)

    attributes = {"run_label": run_label}

    result = None

    df_operations = analyze_operations(run_dir, attributes)
    result = merge_dataframes(result, df_operations)

    df_latency_history = analyze_latency_history(report_dir, run_dir, attributes)
    result = merge_dataframes(result, df_latency_history)

    df_dstat = analyze_dstat(run_dir, attributes)
    result = merge_dataframes(result, df_dstat)

    print(f"Analyzing run_path:{run_dir}: Done")

    return result


def report(report_config: ReportConfig, df: pd.DataFrame):
    if df is None:
        return

    path_csv = f"{report_dir}/data.csv"
    print(f"path csv: {path_csv}")
    df.to_csv(path_csv)

    for column_name in df.columns:
        print(column_name)

    report_operations(report_config, df)
    report_hdr(report_config, df)
    report_dstat(report_config, df)


start_sec = time.time()

report_dir = "/mnt/home/pveentjer/report/"  # tempfile.mkdtemp()
print(f"Report directory {report_dir}")

runs = {}
runs["valuelength_1000"] = "/home/pveentjer/tmp/report/runs/valuelength_1000/04-10-2023_06-35-01"
runs["valuelength_1"] = "/home/pveentjer/tmp/report/runs/valuelength_1/04-10-2023_08-00-07"

report_config = ReportConfig(report_dir, runs, warmup_seconds=warmup_seconds, cooldown_seconds=cooldown_seconds)

prepare(report_config)
df = analyze(report_config)
report(report_config, df)

duration_sec = time.time() - start_sec
log(f"Generating report: Done  (duration {duration_sec:.2f} seconds)")
