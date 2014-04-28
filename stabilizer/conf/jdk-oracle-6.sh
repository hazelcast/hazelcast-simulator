#!/bin/sh

cd ~
wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/jdk-6u45-linux-x64.bin
sh jdk-6u45-linux-x64.bin
echo "export JAVA_HOME=~/jdk1.6.0_45" >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc