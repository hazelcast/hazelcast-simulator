#!/usr/bin/env python3
import os
import sys
import argparse

from simulator.perftest import PerftestCreateCli, PerftestCloneCli, PerftestRunCli, PerftestExecCli, \
    PerftestKillJavaCli, PerftestCollectCli, PerftestCleanCli
from simulator.perftest_report import PerfTestReportCli


usage = '''perftest <command> [<args>]

The available commands are:
    create      Creates a new performance test based on a template.
    clone       Clones an existing performance test.
    collect     Collects the performance test data and stores it in result.yaml.
    exec        Executes a performance test.
    run         Runs a tests.yaml which is a self contained set of tests
    kill_java   Kills all Java processes   
    report      Generate performance report 
'''


# https://stackoverflow.com/questions/27146262/create-variable-key-value-pairs-with-argparse-python


class PerftestCli:

    def __init__(self):
        parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                         description='Management and execution of performance tests', usage=usage)
        parser.add_argument('command', help='Subcommand to run')

        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command', parser.print_help())
            exit(1)

        getattr(self, args.command)()

    def create(self):
        PerftestCreateCli(sys.argv[2:])

    def clean(self):
        PerftestCleanCli(sys.argv[2:])

    def clone(self):
        PerftestCloneCli(sys.argv[2:])

    def run(self):
        PerftestRunCli(sys.argv[2:])

    def exec(self):
        PerftestExecCli(sys.argv[2:])

    def kill_java(self):
        PerftestKillJavaCli(sys.argv[2:])

    def collect(self):
        PerftestCollectCli(sys.argv[2:])

    def report(self):
        PerfTestReportCli(sys.argv[2:])


if __name__ == '__main__':
    os.path.expanduser('~/your_directory')
    PerftestCli()
