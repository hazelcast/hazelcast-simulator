#!/bin/sh

cd ~
wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/openjdk-1.7.0-u45-unofficial-icedtea-2.4.3-linux-amd64-image.zip
unzip -q openjdk-1.7.0-u45-unofficial-icedtea-2.4.3-linux-amd64-image.zip
echo "export JAVA_HOME=~/openjdk-1.7.0-u45-unofficial-icedtea-2.4.3-linux-amd64-image" >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc