#!/bin/bash

set -e
source jdk-support.sh

installPackage wget

JDK_FILE="jdk-6u45-linux-x64.bin"

cd ~
wget --no-verbose -N "$BASE_URL/$JDK_FILE"
chmod +x ${JDK_FILE}
./${JDK_FILE}

addJavaHome "~/jdk1.6.0_45"
