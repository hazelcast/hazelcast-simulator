#!/usr/bin/env python3
import argparse
import os
import statistics
import subprocess
from enum import Enum
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from signal_processing_algorithms.energy_statistics.energy_statistics import e_divisive, get_energy_statistics, \
    get_energy_statistics_and_probabilities

import commit_sorter
from simulator.log import info, log_header
from simulator.util import load_yaml_file, validate_dir, mkdir, validate_git_dir, write, write_yaml, exit_with_error


class TimeSeries:
    def __init__(self,
                 x,
                 y,
                 x_label=None,
                 y_label=None,
                 name=None,
                 increase_is_positive=True):
        assert len(x) == len(y)
        self.x = x
        self.y = y
        self.x_label = x_label
        self.y_label = y_label
        self.name = name
        self.increase_is_positive = increase_is_positive
        self.fake_x = np.arange(0, len(y))

    # Creates a new TimeSeries containing the newest n items.
    def newest(self, n):
        length = len(self.x)
        if length <= n:
            return self

        start = length - n
        x = self.x[start:]
        y = self.y[start:]
        return TimeSeries(x, y, x_label=self.x_label, y_label=self.y_label, name=self.name,
                          increase_is_positive=self.increase_is_positive)

    def __len__(self):
        return len(self.x)


def plot(ts, filename, cps=None, aps=None, ymin=0, width=1600, height=900, title="Changepoint and anomalies"):
    plt.title(f"{title}: {ts.name}")

    my_dpi = 96
    plt.figure(figsize=(width / my_dpi, height / my_dpi), dpi=my_dpi)
    plt.xticks(rotation=90)
    plt.grid()
    plt.xlabel(ts.x_label)
    plt.ylabel(ts.y_label)
    plt.plot(ts.fake_x, ts.y, color="orange")

    if aps:
        for p in aps:
            x = ts.fake_x[p.index]
            y = ts.y[p.index]
            commit = ts.x[p.index]
            if p.direction == Change.POSITIVE:
                plt.plot(x, y, 'o', markersize=12, color="green", label=f"pos. anom.: {commit}")
            else:
                plt.plot(x, y, 'o', markersize=12, color="blue", label=f"neg. anom.: {commit}")

    if cps:
        for p in cps:
            x = ts.fake_x[p.index]
            y = ts.y[p.index]
            commit = ts.x[p.index]
            plt.plot(x, y, 'o', markersize=6, color="red", label=f"change point: {commit}")

    plt.legend()
    plt.ylim(ymin=ymin)
    plt.subplots_adjust(bottom=0.4)
    plt.savefig(filename)


class Change(Enum):
    POSITIVE = 1
    NEGATIVE = 2


# https://netflixtechblog.com/fixing-performance-regressions-before-they-happen-eab2602b86fe
# m is the number of samples
# n is the number of standard deviation away from the mean
def anomaly_detection(ts, min_history_length=1, max_history_length=40, max_n=4):
    if min_history_length > max_history_length:
        raise Exception()

    history = []
    anomalies = []
    for i in range(1, len(ts.x)):
        history.append(ts.y[i - 1])

        if len(history) > max_history_length:
            del history[0]

        if len(history) < min_history_length:
            continue

        mean = statistics.mean(history)
        std = statistics.pstdev(history)
        diff = ts.y[i] - mean
        n = diff / std
        if abs(n) > max_n:
            anomalies.append(AnomalyPoint(i, n))

    return anomalies


class AnomalyPoint:

    # n is the number of standard deviations away from the mean
    def __init__(self, index, n):
        self.index = index
        self.n = n
        self.direction = Change.POSITIVE if n > 0 else Change.NEGATIVE


class ChangePoint:

    def __init__(self, index, direction):
        self.index = index
        self.direction = direction


def load_commit_dir(dir, commit):
    commit_dir = f"{dir}/{commit}"
    result = {}
    for run in os.listdir(commit_dir):
        results_file = f"{commit_dir}/{run}/results.yaml"
        if not os.path.exists(results_file):
            continue

        results_yaml = load_yaml_file(results_file)
        for test, map in results_yaml.items():
            measurements = map['measurements']
            for name, value in measurements.items():
                # hack
                if name == "duration(ms)":
                    continue

                values = result.get(name)
                if not values:
                    values = []
                    result[name] = values
                values.append((commit, value))
    return result


def pick_best_value(values, metric):
    best = None

    # ugly hack; need better mechanism
    if "(us)" in metric:
        for (commit, value) in values:
            if not best or value < best[1]:
                best = (commit, value)
    else:
        for (commit, value) in values:
            if not best or value > best[1]:
                best = (commit, value)

    return best


def ordered_commits(dir, git_dir):
    commits = []
    for file in os.listdir(dir):
        if os.path.isdir(f"{dir}/{file}"):
            filename = os.fsdecode(file)
            commits.append(filename)

    if not commits:
        return []

    return commit_sorter.order(commits, git_dir)


def load_ts_per_metric(dir, git_dir):
    info("Loading data")
    y_map = {}
    x_map = {}
    for commit in ordered_commits(dir, git_dir):
        result = load_commit_dir(dir, commit)

        for metric_name, values in result.items():
            (commit, value) = pick_best_value(values, metric_name)

            y = y_map.get(metric_name)
            x = x_map.get(metric_name)
            if not y:
                y = []
                y_map[metric_name] = y
                x = []
                x_map[metric_name] = x
            y.append(value)
            x.append(commit)

    result = {}
    for metric_name in y_map.keys():
        y = np.array(y_map[metric_name], dtype=float)
        x = np.array(x_map[metric_name])
        result[metric_name] = TimeSeries(x, y, x_label="Commit", y_label=metric_name, name=metric_name)
    return result


def changepoint_detection(ts, permutations=100, pvalue=0.05):
    indices = e_divisive(ts.y, permutations=permutations, pvalue=pvalue)


    if indices:
        i = indices[0]
        x = ts.y[0:i]
        y = ts.y[i:]
        print(get_energy_statistics_and_probabilities(x, y))

    result = []
    for i in indices:
        result.append(ChangePoint(i, None))
    return result


class PerfRegressionAnalysisCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Does performance analysis')
        parser.add_argument("dir", help="The directory containing the results (per commit hash)", nargs=1)
        parser.add_argument("-g", "--git-dir", metavar='git_dir', help="The directory containing the git repo", nargs=1,
                            default=[f"{os.getcwd()}/.git"])
        parser.add_argument("-d", "--debug", help="Print debug info", action='store_true')
        parser.add_argument("-z", "--zero", help="Plot from zero", action='store_true')
        parser.add_argument("-l", "--latest", nargs=1, help="Take the n latest items", type=int)
        parser.add_argument("--width", nargs=1, help="The width of the images", type=int, default=1600)
        parser.add_argument("--height", nargs=1, help="The height of the images", type=int, default=900)
        parser.add_argument("--permutations", nargs=1,
                            help="The number of permutations for the change point detection", type=int, default=100)
        parser.add_argument("--pvalue", nargs=1,
                            help="The pvalue for the change point detection", type=float, default=0.05)
        parser.add_argument("--n_std_treshhold", nargs=1,
                            help="The number of standard deviations away from the mean to be considered an anomaly",
                            type=float, default=4)
        parser.add_argument("-o", "--output", help="The directory to write the output", nargs=1,
                            default=f"{os.getcwd()}/analysis")

        log_header("perfregtest analysis")

        args = parser.parse_args(argv)
        self.git_dir = validate_git_dir(args.git_dir[0])
        self.latest = args.latest
        self.width = args.width
        self.height = args.height
        self.dir = validate_dir(args.dir[0])
        self.output = mkdir(args.output)
        self.zero = args.zero
        self.ts_per_metric = load_ts_per_metric(self.dir, self.git_dir)
        self.permutations = args.permutations
        if self.permutations < 1:
            exit_with_error("permutations can't be smaller than 1")
        self.pvalue = args.pvalue
        if self.pvalue < 0:
            exit_with_error("pvalue can't be smaller than 0")
        if self.pvalue > 1:
            exit_with_error("pvalue can't be larger than 1")
        self.anomalies_per_metric = {}
        self.changepoints_per_metric = {}
        self.n_std_treshhold = args.n_std_treshhold

        self.trim()
        self.changepoint_detection()
        self.anomaly_detection()
        self.make_plots()
        self.make_report()

        info(f"Analysis results can be found in [{self.output}].")

    def make_report(self):
        info("Making report")
        all_problems = {}
        commits = set()
        for metric, ts in self.ts_per_metric.items():
            cps = self.changepoints_per_metric.get(metric)
            aps = self.anomalies_per_metric.get(metric)
            for cp in cps:
                commit = ts.x[cp.index]
                commits.add(commit)

                commit_problems = all_problems.get(commit)
                if not commit_problems:
                    commit_problems = []
                    all_problems[commit] = commit_problems

                commit_problems.append(f"{metric} change-point")
            for ap in aps:
                commit = ts.x[ap.index]
                commits.add(commit)

                commit_problems = all_problems.get(commit)
                if not commit_problems:
                    commit_problems = []
                    all_problems[commit] = commit_problems

                x = "{:.2f}".format(ap.n)
                msg = f"{x} std away from the mean."
                commit_problems.append(f"{metric} anomaly : {msg}")
        commits = commit_sorter.order(commits, self.git_dir)

        yaml_content = {}
        text = ""
        for commit in commits:
            commit_problems = all_problems.get(commit)
            if not commit_problems:
                continue

            text += f"commit {commit}\n"
            for problem in commit_problems:
                text += f"   {problem}\n"
            yaml_content[commit] = commit_problems
        write(f"{self.output}/analysis.txt", text)
        write_yaml(f"{self.output}/analysis.yaml", yaml_content)

    def make_plots(self):
        info("Making plots")
        for metric, ts in self.ts_per_metric.items():
            cps = self.changepoints_per_metric.get(metric)
            aps = self.anomalies_per_metric.get(metric)
            ymin = 0 if self.zero else None
            filename = f"{self.output}/{metric}.png"
            plot(ts, filename, cps=cps, aps=aps, ymin=ymin, width=self.width, height=self.height)

    def anomaly_detection(self):
        info("Anomaly detection")
        for metric, ts in self.ts_per_metric.items():
            aps = anomaly_detection(ts, min_history_length=10, max_n=self.n_std_treshhold)
            self.anomalies_per_metric[metric] = aps

    def changepoint_detection(self):
        info("Changepoint detection")
        for metric, ts in self.ts_per_metric.items():
            cps = changepoint_detection(ts, self.permutations, self.pvalue)
            self.changepoints_per_metric[metric] = cps

    def trim(self):
        info("Trimming data")
        if self.latest:
            for metric, ts in self.ts_per_metric.items():
                self.ts_per_metric[metric] = ts.newest(self.latest)


class PerfRegressionSummaryCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Does performance analysis')
        parser.add_argument("dir", help="The directory containing the results (per commit hash)", nargs=1)
        parser.add_argument("-g", "--git-dir", metavar='git_dir', help="The directory containing the git repo", nargs=1,
                            default=[f"{os.getcwd()}/.git"])

        args = parser.parse_args(argv)
        git_dir = validate_git_dir(args.git_dir[0])
        dir = validate_dir(args.dir[0])

        commits = ordered_commits(dir, git_dir)
        for commit in commits:
            cmd = f"""git --git-dir {git_dir} show -s --format='%H | %ai' {commit}"""
            out = subprocess.check_output(cmd, shell=True, text=True).strip()
            result_count = sum(r is r for r in Path(f"{dir}/{commit}").rglob('results.yaml'))
            print(f"{out} results={result_count}")
        return
