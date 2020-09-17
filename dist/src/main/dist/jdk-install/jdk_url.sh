#!/bin/bash

set -e
source jdk-support.sh

installPackage wget

JDK_URL=%JDK_URL%
FILE_NAME=$(echo "$JDK_URL" | sed 's:.*/::')

cd ~

if [ ! -f "$FILE_NAME" ]; then
  echo "[INFO] Downloading $JDK_URL"
  wget -N --no-verbose  --no-check-certificate -c --header "Cookie: oraclelicense=accept-securebackup-cookie"  "$JDK_URL"
else
  echo "[INFO] Skip download $JDK_URL; file already downloaded."
fi

# extract the directory name
JAVA_HOME=$(tar -tzf "$FILE_NAME" | head -1 | cut -f1 -d"/")
echo JAVA_HOME "$JAVA_HOME"
rm -fr "$JAVA_HOME"

tar xfz "$FILE_NAME"

addJavaHome "$JAVA_HOME"