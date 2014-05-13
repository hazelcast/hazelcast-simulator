#!/bin/bash

set -e

source jdk-support.sh

install wget
install tar

cd ~
wget --no-verbose http://ec2-54-87-52-100.compute-1.amazonaws.com/jdk-7u55-linux-x64.tar.gz
tar xfz jdk-7u55-linux-x64.tar.gz

prepend 'export PATH=$JAVA_HOME/bin:$PATH'  ~/.bashrc
prepend "export JAVA_HOME=~/jdk1.7.0_55"  ~/.bashrc

