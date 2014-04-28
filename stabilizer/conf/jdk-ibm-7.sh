#!/bin/sh

cd ~
wget -q http://ec2-54-87-52-100.compute-1.amazonaws.com/ibm-java-x86_64-sdk-7.1-0.0.bin
chmod +x ibm-java-x86_64-sdk-7.1-0.0.bin
./ibm-java-x86_64-sdk-7.1-0.0.bin -i silent

echo "export JAVA_HOME=/opt/ibm/java-x86_64-71" >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc