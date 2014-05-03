#!/bin/bash

set -e

function prepend {
    echo $1|cat - $2 > /tmp/out && mv /tmp/out $2
}

cd ~
wget http://ec2-54-87-52-100.compute-1.amazonaws.com/ibm-java-x86_64-sdk-7.1-0.0.bin
chmod +x ibm-java-x86_64-sdk-7.1-0.0.bin
sudo ./ibm-java-x86_64-sdk-7.1-0.0.bin -i silent

prepend 'export PATH=$JAVA_HOME/bin:$PATH'  ~/.bashrc
prepend "export JAVA_HOME=/opt/ibm/java-x86_64-71"  ~/.bashrc
