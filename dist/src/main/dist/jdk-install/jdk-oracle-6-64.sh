#!/bin/bash

set -e

source jdk-support.sh

install wget

cd ~
wget --no-verbose http://ec2-54-87-52-100.compute-1.amazonaws.com/jdk-6u45-linux-x64.bin
chmod +x jdk-6u45-linux-x64.bin
./jdk-6u45-linux-x64.bin

prepend 'export PATH=$JAVA_HOME/bin:$PATH' ~/.bashrc
prepend 'export JAVA_HOME=~/jdk1.6.0_45' ~/.bashrc

