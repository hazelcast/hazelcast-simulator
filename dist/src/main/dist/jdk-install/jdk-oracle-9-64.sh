#!/bin/bash

set -e

source jdk-support.sh

install wget
install tar

cd ~
wget --no-verbose http://ec2-54-87-52-100.compute-1.amazonaws.com/jdk-9-ea+157_linux-x64_bin.tar.gz
tar xfz jdk-9-ea+157_linux-x64_bin.tar.gz

prepend 'export PATH=$JAVA_HOME/bin:$PATH'  ~/.bashrc
prepend "export JAVA_HOME=~/jdk-9"  ~/.bashrc
