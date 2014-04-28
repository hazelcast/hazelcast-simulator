#!/bin/bash

set -e

function prepend {
    echo $1|cat - $2 > /tmp/out && mv /tmp/out $2
}

cd ~
wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/openjdk-1.6.0-unofficial-b30-linux-amd64-image.zip
unzip -q openjdk-1.6.0-unofficial-b30-linux-amd64-image.zip

prepend 'export PATH=$JAVA_HOME/bin:$PATH'  ~/.bashrc
prepend "export JAVA_HOME=~/openjdk-1.6.0-unofficial-b30-linux-amd64-image"  ~/.bashrc