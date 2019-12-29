#!/bin/sh

agent-ssh "sudo yum -y install perf"
agent-ssh "sudo yum -y install git"
agent-ssh "sudo yum -y install cmake"
agent-ssh "sudo yum -y install make"
agent-ssh "sudo yum -y install gcc-c++"

