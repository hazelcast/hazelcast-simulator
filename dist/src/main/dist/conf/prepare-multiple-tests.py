#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os

parser = argparse.ArgumentParser(description='Prepare a template for multiple tests info file.')
parser.add_argument('testInformationFile', nargs=1, type=str,
                    help='a txt file that has the required test info')

args = parser.parse_args()
testInfoFile = args.testInformationFile[0]

simulator_home = os.environ['SIMULATOR_HOME']
os.environ['LC_CTYPE'] = "en_US.UTF-8"

templateHeader = "## name, testFilePath, memberCount, clientCount, ratePerSecond," \
                 " threadCount, duration, warmup, cooldown, fieldName1, fieldValue1, fieldName2, fieldValue2 ... \n"

if os.path.isfile(testInfoFile):
    with open(testInfoFile) as f:
        firstLine = f.readline()
        if firstLine != templateHeader:
            print("Test Info File doesn't have the templateHeader, creating a new file with appendix '-new'")
            with open(testInfoFile + "-new", "x") as n:
                n.write(templateHeader)
            print("New Template file created successfully!")
        else:
            print("A template file already exist with given name!")
else:
    print("No Test Info File found with given name, a new one is being created!")
    with open(testInfoFile, "x") as n:
        n.write(templateHeader)
    print("New Template file created successfully!")
