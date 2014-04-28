#!/bin/bash

set -e

function prepend {
    echo $1|cat - $2 > /tmp/out && mv /tmp/out $2
}

cd ~
wget http://ec2-54-87-52-100.compute-1.amazonaws.com/ibm-java-x86_64-sdk-6.0-15.1.bin
chmod +x ibm-java-x86_64-sdk-6.0-15.1.bin
./ibm-java-x86_64-sdk-6.0-15.1.bin -i silent

prepend 'export PATH=$JAVA_HOME/bin:$PATH'  ~/.bashrc
prepend "export JAVA_HOME=/opt/ibm/java-x86_64-60/"  ~/.bashrc
