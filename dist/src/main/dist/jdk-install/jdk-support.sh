#!/bin/bash

# Download sources
#
# IBM
# http://public.dhe.ibm.com/ibmdl/export/pub/systems/cloud/runtimes/java/
#
# OpenJDK
# https://github.com/alexkasko/openjdk-unofficial-builds
# https://github.com/ojdkbuild/ojdkbuild
# https://adopt-openjdk.ci.cloudbees.com/job/openjdk-1.9-linux-x86_64/lastSuccessfulBuild/artifact/openjdk-1.9-linux-x86_64-jdk.tar.gz
#
# Oracle
# http://download.oracle.com/otn/java/jdk/6u45-b06/jdk-6u45-linux-x64.bin
# http://download.oracle.com/otn/java/jdk/7u80-b15/jdk-7u80-linux-x64.tar.gz
# http://download.oracle.com/otn-pub/java/jdk/8u121-b13/e9e7ea248e2c4826b92b3f075a80e441/jdk-8u121-linux-x64.tar.gz
# http://www.java.net/download/java/jdk9/archive/158/binaries/jdk-9-ea+158_linux-x64_bin.tar.gz
#
# Zulu
# http://cdn.azul.com/zulu/bin/zulu7.17.0.5-jdk7.0.131-linux_x64.tar.gz
# http://cdn.azul.com/zulu/bin/zulu8.20.0.5-jdk8.0.121-linux_x64.tar.gz
#

set -e
#set -x

BASE_URL="http://ec2-54-87-52-100.compute-1.amazonaws.com"

function prepend {
    echo $1 | cat - $2 > /tmp/out && mv /tmp/out $2
}

function installPackage {
    PACKAGE=$1

    if hash ${PACKAGE} 2>/dev/null; then
        echo "$PACKAGE already installed"
        return 0
    fi

    if hash apt-get 2>/dev/null; then
        echo "apt-get is available!"
        sudo apt-get update
        sudo apt-get install -y ${PACKAGE}
    elif hash yum 2>/dev/null; then
        echo "yum is available!"
        sudo yum -y install ${PACKAGE}
    else
        echo "apt-get AND yum are not available!"
    fi
}

function installIbmJdk {
    JDK_FILE=$1
    TMP_JAVA_HOME=$2

    installPackage wget

    echo "Installing IBM JDK $JDK_FILE"

    echo "INSTALLER_UI=silent
USER_INSTALL_DIR=$TMP_JAVA_HOME
LICENSE_ACCEPTED=TRUE" >> ~/installer.properties

    cd ~
    wget --no-verbose -N "$BASE_URL/$JDK_FILE"
    chmod +x ${JDK_FILE}
    sudo ./${JDK_FILE} -r installer.properties

    addJavaHome ${TMP_JAVA_HOME}
}

function installOpenJdk {
    JDK_BASE_NAME=$1
    TMP_JAVA_HOME=$2

    installPackage wget
    installPackage unzip

    echo "Installing OpenJDK $JDK_BASE_NAME"

    cd ~
    wget --no-verbose -N "$BASE_URL/$JDK_BASE_NAME.zip"
    unzip -q -o "$JDK_BASE_NAME.zip"

    addJavaHome ${TMP_JAVA_HOME}
}

function installOracleJdk {
    JDK_FILE=$1
    TMP_JAVA_HOME=$2

    installPackage wget
    installPackage tar

    echo "Installing Oracle JDK $JDK_FILE"

    cd ~
    wget --no-verbose -N "$BASE_URL/$JDK_FILE"
    tar xfz ${JDK_FILE}

    addJavaHome ${TMP_JAVA_HOME}
}

function installZuluJdk {
    JDK_VERSION=$1
    JDK_FILE=$2
    TMP_JAVA_HOME=$3

    installPackage wget
    installPackage tar

    echo "Installing Zulu JDK $JDK_FILE"

    cd ~
    wget --no-verbose -N --referer=http://www.azul.com/downloads/zulu/zulu-linux ${JDK_FILE} -O zulu-${JDK_VERSION}.tar.gz
    tar xfz zulu-${JDK_VERSION}.tar.gz

    addJavaHome ${TMP_JAVA_HOME}
}

function addJavaHome {
    TMP_JAVA_HOME=$1

    if grep -q "JAVA_HOME=" ~/.bashrc; then
        echo "Updating Java version to $TMP_JAVA_HOME"
        sed -i "s|JAVA_HOME=.*|JAVA_HOME=${TMP_JAVA_HOME}|" ~/.bashrc
    else
        echo "Installing Java version to $TMP_JAVA_HOME"
        prepend 'export PATH=$JAVA_HOME/bin:$PATH' ~/.bashrc
        prepend "export JAVA_HOME=$TMP_JAVA_HOME" ~/.bashrc
    fi
}
