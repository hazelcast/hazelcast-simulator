#!/bin/bash

set -e
source jdk-support.sh

installPackage wget
installPackage tar

cd ~
wget --no-verbose --referer=http://www.azulsystems.com/products/zulu/downloads http://cdn.azulsystems.com/zulu/bin/zulu1.8.0_65-8.10.0.1-x86lx64.zip -O zulu-8.zip
unzip zulu-8.zip

"~/zulu1.8.0_65-8.10.0.1-x86lx64"
