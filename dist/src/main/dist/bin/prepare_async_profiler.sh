#!/bin/bash

# Installation script for the async profiler
# https://github.com/jvm-profiling-tools/async-profiler

version=1.5

agent-ssh "rm -fr async-profiler"
agent-ssh "mkdir async-profiler"
agent-ssh "wget https://github.com/jvm-profiling-tools/async-profiler/releases/download/v${version}/async-profiler-${version}-linux-x64.tar.gz"
agent-ssh "tar -xzvf async-profiler-${version}-linux-x64.tar.gz  --directory async-profiler"
agent-ssh "echo 1 | sudo tee /proc/sys/kernel/perf_event_paranoid"
agent-ssh "echo 0 | sudo tee /proc/sys/kernel/kptr_restrict"
