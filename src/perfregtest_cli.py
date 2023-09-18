#!/usr/bin/env python3
import argparse
import os
import random
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import commit_sampler
import simulator.util
from commit_sampler import CommitSamplerCli
from commit_sorter import CommitOrderCli
from perf_analysis_cli import PerfRegressionAnalysisCli, PerfRegressionSummaryCli
from simulator.log import info, log_header, warn
from simulator.perftest import PerfTest
from simulator.util import shell_logged, now_seconds, validate_dir, load_yaml_file, exit_with_error

default_tests_path = 'tests.yaml'
logfile_name = "run.log"

usage = '''perfregtest <command> [<args>]

The available commands are:
    analyze             Analyzes the results of performance regression tests..
    commit_sampler      Creates a sample of commits between a start and end commit.
    commit_order        Returns an ordered list (from old to new) of commits.
    run                 Runs performance regression tests.
    summary             Shows a summary of a set of performance regression tests
'''


def get_project_version(project_path):
    cmd = f"""
         set -e
         cd {project_path}
         mvn -q -Dexec.executable=echo -Dexec.args='${{project.version}}' --non-recursive exec:exec
         """
    return subprocess.check_output(cmd, shell=True, text=True).strip()


@dataclass
class BuildStats:
    count: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0


def build(commit, build_path):
    info(f"Building {build_path} {commit}")
    start = now_seconds()
    exitcode = shell_logged(f"""
        set -e
        cd {build_path}
        git fetch --all --tags
        git reset --hard
        git checkout {commit}
        mvn clean install -Dmaven.test.skip=true -Dquick
    """, log_file_path=logfile_name)
    if exitcode == 0:
        info(f"Build time: {now_seconds() - start}s")
        return True
    else:
        return False


def build_all(commit_tuple, build_paths, build_stats: BuildStats):
    success = True
    build_stats.count += 1
    remaining = len(commit_tuple)
    for i in range(0, len(commit_tuple)):
        commit = commit_tuple[i]
        build_path = build_paths[i]
        broken_builds_dir = simulator.util.mkdir("broken-builds")
        build_error_file = f"{broken_builds_dir}/{':'.join(commit_tuple)} "
        if os.path.exists(build_error_file):
            build_stats.failed += remaining
            build_stats.skipped += remaining
            warn(f"Skipping {build_path} {commit}, previous build-failure found.")
            return False

        if not build(commit, build_path):
            warn(f"Failed to build {build_path} {commit}.")
            build_stats.failed += remaining
            build_stats.skipped += remaining - 1
            open(build_error_file, "w").close()
            return False
        remaining -= 1
        build_stats.success += 1

    return success


def run(test, commit, runs, project_path, debug=False):
    version = get_project_version(project_path)
    test_name = test['name']
    if test.get('version'):
        warn(f"Ignoring version [{test['version']} in test [{test_name}]")
    test['version'] = f"maven={version}"
    commit_dir = f"runs/{test_name}/{commit}"
    info(f"Running {commit_dir}, runs {runs} ")
    info(f"Version:[{version}]")
    info(f"Test Duration: {test.get('duration')}")

    perftest = PerfTest(logfile=logfile_name, log_shell_command=debug, exit_on_error=False)
    for i in range(0, runs):
        dt = datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        run_path = f"{commit_dir}/{dt}"
        info(f"{i + 1} {run_path}")

        exitcode, run_path = perftest.run_test(test, run_path=run_path)
        if exitcode == 0:
            perftest.collect(f"{run_path}",
                             {'commit': commit, "testname": test_name},
                             warmup_seconds=test.get('warmup_seconds'),
                             cooldown_seconds=test.get('cooldown_seconds'))
        else:
            info("Test failure was detected")


def run_all(commit_tuples, runs, build_paths, tests, debug):
    if not tests:
        exit_with_error("No tests found.")

    if not commit_tuples:
        exit_with_error("No commits found.")

    info(f"Number of builds {len(commit_tuples)}")
    info(f"Tests to execute: {[t['name'] for t in tests]}")

    start = now_seconds()
    build_stats = BuildStats()
    for commitIndex, commit_tuple in enumerate(commit_tuples):
        build_success = False
        commit = commit_tuple[-1]
        c = commit_sampler.to_full_commit(f"{build_paths[-1]}/.git", commit)
        if not c.startswith(commit):
            info(f"{commit}->{c}")
            commit = c

        for test in tests:
            test_name = test['name']
            commit_dir = f"runs/{test_name}/{commit}"
            result_count = sum(r is r for r in Path(commit_dir).rglob('results.yaml'))
            remaining = runs - result_count

            if remaining <= 0:
                info(f"Skipping commit {commit}, test {test_name}, sufficient runs")
                continue

            log_header(f"Commit {commit}")
            start_test = now_seconds()

            if not build_success:
                if build_all(commit_tuple, build_paths, build_stats):
                    build_success = True
                else:
                    info(f"Build failed {build_stats.failed}/{build_stats.count}, skipping runs.")
                    break

            run(test, commit, remaining, build_paths[-1], debug)
            info(f"Testing {test_name} took {now_seconds() - start_test}s")
    duration = now_seconds() - start
    info(f"Duration: {duration}s")
    info(f"Builds total: {build_stats.count}")
    info(f"Builds failed: {build_stats.failed}")
    info(f"Builds skipped: {build_stats.skipped}")
    info(f"Builds succeeded: {100 * build_stats.success / build_stats.count}%")


class PerfRegTestRunCli:

    def __init__(self, argv):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Runs performance regression tests based on a series of commits')
        parser.add_argument("paths", nargs=1, help="The paths of the directories to build")
        parser.add_argument("commits", nargs="+", help="The commits to build. When there are multiple paths, "
                                                       "a commit for each path can passed using a:b:... ")
        parser.add_argument('-f', '--file', nargs=1, help='The tests file', default=[default_tests_path])
        parser.add_argument("-r", "--runs", nargs=1, help="The number of runs per commit",
                            default=[3], type=int)
        parser.add_argument('-t', '--test', nargs=1,
                            help='The names of the tests to run. By default all tests are run.')
        parser.add_argument("--randomize", help="Randomizes the commits", action='store_true')
        parser.add_argument("-d", "--debug", help="Print debug info", action='store_true')

        args = parser.parse_args(argv)
        runs = args.runs[0]
        build_paths = args.paths[0].split(':')
        for build_path in build_paths:
            validate_dir(build_path)

        commit_tuples = []
        for t in args.commits:
            tuple = t.split(":")
            if len(tuple) != len(build_paths):
                exit_with_error(
                    f"The number of commits in {t} doesn't match the number of build paths {':'.join(build_paths)}")
            commit_tuples.append(tuple)

        if args.randomize:
            random.shuffle(commit_tuples)

        tests = load_yaml_file(args.file[0])
        filtered_tests = []
        if args.test:
            for test in tests:
                test_name = test['name']
                if test_name in args.test:
                    filtered_tests.append(test)
        else:
            filtered_tests = tests

        run_all(commit_tuples, runs, build_paths, filtered_tests, args.debug)


class PerfRegtestCli:

    def __init__(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Contains tools for performance regression testing.', usage=usage)
        parser.add_argument('command', help='Subcommand to run')

        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command', parser.print_help())
            exit(1)

        getattr(self, args.command)(sys.argv[2:])

    def run(self, argv):
        PerfRegTestRunCli(argv)

    def commit_sampler(self, argv):
        CommitSamplerCli(argv)

    def commit_sorter(self, argv):
        CommitOrderCli(argv)

    def analyze(self, argv):
        PerfRegressionAnalysisCli(argv)

    def summary(self, argv):
        PerfRegressionSummaryCli(argv)


if __name__ == '__main__':
    os.path.expanduser('~/your_directory')
    PerfRegtestCli()
