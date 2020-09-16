#!/bin/bash

set -e
source jdk-support.sh

version=%VERSION%

installPackage wget
installPackage tar

echo "Installing Zulu JDK $version"
cd ~

if [ ! -d $version ]; then
    wget --no-verbose -N --referer=http://www.azul.com/downloads/zulu/zulu-linux http://cdn.azul.com/zulu/bin/${version}.tar.gz -O ${version}.tar.gz
    tar xfz ${version}.tar.gz
fi

addJavaHome ${version}
