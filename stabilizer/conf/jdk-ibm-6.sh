#!/bin/sh

cd ~
wget http://ec2-54-87-52-100.compute-1.amazonaws.com/ibm-java-x86_64-sdk-6.0-15.1.bin
chmod +x ibm-java-x86_64-sdk-6.0-15.1.bin
./ibm-java-x86_64-sdk-6.0-15.1.bin -i silent

echo "export JAVA_HOME=/opt/ibm/java-x86_64-60/" >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc