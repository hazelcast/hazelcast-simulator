#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import subprocess
import os
import shutil
import fnmatch
from time import gmtime, strftime


parser = argparse.ArgumentParser(description='Runs multiple tests sequentially and reports them using the given workDir and testInfos')
parser.add_argument('testInformationFile', nargs=1, type=str,
                    help='a txt file that has the required test info')

args = parser.parse_args()
testInfoFile = args.testInformationFile[0]

simulator_home = os.environ['SIMULATOR_HOME']
os.environ['LC_CTYPE'] = "en_US.UTF-8"

# name, testFilePath, memberCount, clientCount, ratePerSecond, threadCount, duration, warmup, cooldown, (fieldName, fieldValue) ...
column_count = 9
backup_folder_name = "old-runs-backup"

report_folder_name = "multiple-tests-" + strftime("%Y-%m-%d__%H-%M-%S", gmtime())


if not os.path.isfile(testInfoFile):
    print("No Test Info File found with given name, aborting!")
    exit(1)

# TODO keep logs of runs and print them to files
# TODO add check for if current dir workdir
# TODO backup run and test.properties files beforehands


def parse_tokens(line: str):
    tokens = []
    lastToken = ""
    for i in range(len(line)):
        cha = line[i]
        if cha == "," or i == len(line)-1:
            if i == len(line)-1:
                lastToken += cha
            tokens.append(lastToken.strip())
            lastToken = ""
        else:
            lastToken += cha
    return tokens


def prepare():
    subprocess.run(["./prepare"])


def prepare_test_properties(testFilePath, ratePerSecond, threadCount, fields):
    with open("test.properties", "w") as f:
        f.write("class = {}\n".format(testFilePath))
        f.write("threadCount = {}\n".format(threadCount))
        if int(ratePerSecond) > 0:
            f.write("ratePerSecond = {}\n".format(ratePerSecond))
        for field in fields:
            f.write("{} = {}\n".format(field[0], field[1]))


def prepare_run_script(memberCount, clientCount, duration):
    new_file = ""
    with open("run") as run:
        for line in run.readlines():
            if line.startswith("members"):
                new_file += "members=${1:-" + memberCount + "}\n"
            elif line.startswith("clients"):
                new_file += "clients=${2:-" + clientCount + "}\n"
            elif line.startswith("duration"):
                new_file += "duration=${3:-" + duration + "}\n"
            else:
                new_file += line

    with open("run", "w") as run:
        run.write(new_file)


def run_test(testFilePath, memberCount, clientCount, ratePerSecond, threadCount, duration, fields):
    print("Running Test ..")
    prepare_test_properties(testFilePath, ratePerSecond, threadCount, fields)
    prepare_run_script(memberCount, clientCount, duration)
    print("'run' script and 'test.properties' updated accordingly!")
    subprocess.run(["./run"])
    print("Finished Running Test!")


def prepare_report(test_name, warmup, cooldown):
    print("Preparing report..")
    files = [f for f in os.listdir('.')]
    os.makedirs(os.path.join(report_folder_name, test_name))
    for name in files:
        if fnmatch.fnmatch(name, "*-*-*__*_*_*"):
            shutil.move(name, os.path.join(report_folder_name, test_name, test_name+"-run"))
    subprocess.run(["benchmark-report", "-w", str(warmup), "-c", str(cooldown), "-o",
                    os.path.join(report_folder_name, test_name, test_name+"-report"),
                    os.path.join(report_folder_name, test_name, test_name+"-run")])
    print("Finished report generation!")


def backup_workdir():
    print("Backing up related files in workDir...")
    shutil.copyfile("run", "run-backup")
    shutil.copyfile("test.properties", "test.properties-backup")
    os.mkdir(backup_folder_name)
    files = [f for f in os.listdir('.')]
    for name in files:
        if fnmatch.fnmatch(name, "*-*-*__*_*_*"):
            shutil.move(name, os.path.join(backup_folder_name, name))


def restore_backup_workdir():
    print("Returning workDir to initial state...")
    shutil.copyfile("run-backup", "run")
    shutil.copyfile("test.properties-backup", "test.properties")

    files = [f for f in os.listdir(backup_folder_name)]
    for name in files:
        shutil.move(os.path.join(backup_folder_name, name), os.path.join(os.getcwd(), name))
    os.remove(backup_folder_name)
    print("Workdir restoration completed!!")






print("Prepare-Phase -->> upload simulator to agents")
prepare()

backup_workdir()

print("Run phase started!")
with open(testInfoFile) as f:
    lines = f.readlines()
    for line in lines:
        print("Processing line: ", line)
        if len(line.strip()) == 0:
            continue
        if line.startswith("##"):
            print("Line is commented out, skipping..")
            continue
        print("Parsing line..")
        try:
            tokens = parse_tokens(line)
            fieldCount = int((len(tokens) - column_count)/2)
            fields = []
            for i in range(fieldCount):
                name = tokens[column_count+i*2]
                value = tokens[column_count+i*2+1]
                fields.append((name, value))

            print("Name Of The Test: ", tokens[0])
            print("Test Class: ", tokens[1])
            print("Member Count: ", tokens[2])
            print("Client Count: ", tokens[3])
            print("Rate Per Second: ", tokens[4])
            print("Thread Count: ", tokens[5])
            print("Duration: ", tokens[6])
            print("Warmup(secs): ", tokens[7])
            print("Cooldown(secs): ", tokens[8])
            print("Fields: ", fields)

            run_test(tokens[1], tokens[2], tokens[3], tokens[4], tokens[5], tokens[6], fields)

            prepare_report(tokens[0], tokens[7], tokens[8])
        except:
            print("An exception occurred during the processing of the current line skipping...")

print("All tests are completed!!")
restore_backup_workdir()
