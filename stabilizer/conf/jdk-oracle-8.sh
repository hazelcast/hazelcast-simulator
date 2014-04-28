#!/bin/bash

cd ~
wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/jdk-8u5-linux-x64.tar.gz
tar xfz jdk-8u5-linux-x64.tar.gz
echo "export JAVA_HOME=~/jdk1.8.0_05" >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc

