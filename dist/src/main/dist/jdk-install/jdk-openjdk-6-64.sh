#!/bin/bash

set -e

source jdk-support.sh

install wget
install unzip

cd ~
wget --no-verbose http://ec2-54-87-52-100.compute-1.amazonaws.com/openjdk-1.6.0-unofficial-b30-linux-amd64-image.zip
unzip -q openjdk-1.6.0-unofficial-b30-linux-amd64-image.zip

prepend 'export PATH=$JAVA_HOME/bin:$PATH'  ~/.bashrc
prepend "export JAVA_HOME=~/openjdk-1.6.0-unofficial-b30-linux-amd64-image"  ~/.bashrc