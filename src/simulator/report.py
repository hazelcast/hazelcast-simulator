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


def analyze_run(report_dir, run_dir, run_label=None):
    print(f"Analyzing run_path:{run_dir}")

    if run_label is None:
        run_label = os.path.basename(run_dir)

    attributes = {"run_label": run_label}

    result = None

    df_operations = analyze_operations_data(run_dir, attributes)
    result = merge_dataframes(result, df_operations)

    df_latency_history = analyze_latency_history(report_dir, run_dir, attributes)
    result = merge_dataframes(result, df_latency_history)

    df_dstat = analyze_dstat(run_dir, attributes)
    result = merge_dataframes(result, df_dstat)

    print(f"Analyzing run_path:{run_dir}: Done")

    return result


def make_report(df):
    if df is None:
        return

    path_csv = f"{report_dir}/data.csv"
    print(f"path csv: {path_csv}")
    df.to_csv(path_csv)

    for column_name in df.columns:
        print(column_name)

    report_operations(report_dir, df)
    report_latency_history(report_dir, df)
    report_dstat(report_dir, df)


start_sec = time.time()

report_dir = "/mnt/home/pveentjer/report/"  # tempfile.mkdtemp()
mkdir(report_dir)
print(f"Report directory {report_dir}")

runs = {}
runs["valuelength_1000"] = "/home/pveentjer/tmp/report/runs/valuelength_1000/04-10-2023_06-35-01"
runs["valuelength_1"] = "/home/pveentjer/tmp/report/runs/valuelength_1/04-10-2023_08-00-07"

df = None

for run_label, run_dir in runs.items():
    if len(runs) == 1:
        df = analyze_run(report_dir, run_dir, run_label)
    else:
        tmp_df = analyze_run(report_dir, run_dir, run_label)
        df = merge_dataframes(df, shift_to_epoch(tmp_df))

make_report(df)

hgrm_files = set()
for run_label, run_dir in runs.items():
    hgrm_files.update(find_hgrm_files(report_dir, run_label))
report_hgrm(report_dir, runs, hgrm_files)

duration_sec = time.time() - start_sec
log(f"Generating report: Done  (duration {duration_sec:.2f} seconds)")
