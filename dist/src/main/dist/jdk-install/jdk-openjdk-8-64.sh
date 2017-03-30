#!/bin/bash

set -e

source jdk-support.sh

installPackage wget
installPackage unzip

cd ~
wget --no-verbose http://ec2-54-87-52-100.compute-1.amazonaws.com/java-1.8.0-openjdk-1.8.0.121-0.b13.el6_8.x86_64.zip
unzip -q  java-1.8.0-openjdk-1.8.0.121-0.b13.el6_8.x86_64.zip

prepend 'export PATH=$JAVA_HOME/bin:$PATH'  ~/.bashrc
prepend "export JAVA_HOME=~/java-1.8.0-openjdk-1.8.0.121-0.b13.el6_8.x86_64"  ~/.bashrc
