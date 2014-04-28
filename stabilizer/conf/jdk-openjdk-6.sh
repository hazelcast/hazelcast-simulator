#!/bin/sh

set -e


cd ~
wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/openjdk-1.6.0-unofficial-b30-linux-amd64-image.zip
unzip -q openjdk-1.6.0-unofficial-b30-linux-amd64-image.zip
echo "export JAVA_HOME=~/openjdk-1.6.0-unofficial-b30-linux-amd64-image" >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc