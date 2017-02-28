#!/bin/bash

set -e
source jdk-support.sh

installPackage wget
installPackage tar

cd ~
wget --no-verbose --referer=http://www.azulsystems.com/products/zulu/downloads http://cdn.azulsystems.com/zulu/bin/zulu1.7.0_91-7.12.0.3-x86lx64.zip -O zulu-7.zip
unzip zulu-7.zip

addJavaHome "~/zulu1.7.0_91-7.12.0.3-x86lx64"
