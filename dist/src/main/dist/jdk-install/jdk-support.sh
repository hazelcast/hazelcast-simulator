#!/bin/bash

# Download sources
#
# IBM
# http://public.dhe.ibm.com/ibmdl/export/pub/systems/cloud/runtimes/java/
#

set -e
#set -x

BASE_URL="https://s3.amazonaws.com/simulator-jdk"

function prepend {
    echo $1 | cat - $2 > /tmp/out && mv /tmp/out $2
}

function installPackage {
    PACKAGE=$1

    if hash ${PACKAGE} 2>/dev/null; then
        echo "$PACKAGE already installed"
        return 0
    fi

    CAN_I_RUN_SUDO=$(sudo -n uptime 2>&1 | grep "load" | wc -l)
    if [ ${CAN_I_RUN_SUDO} -le 0 ]; then
        echo "Cannot install '$PACKAGE', since '$USER' is not allowed to use sudo!"
        return 1
    fi

    if hash apt-get 2>/dev/null; then
        echo "apt-get is available!"
        sudo apt-get update
        sudo apt-get install -y ${PACKAGE}
    elif hash yum 2>/dev/null; then
        echo "yum is available!"
        sudo yum -y install ${PACKAGE}
    else
        echo "Cannot install '$PACKAGE', since apt-get AND yum are not available!"
        return 1
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
    VERSION=$1

    installPackage wget
    installPackage tar

    echo "Installing Zulu JDK $VERSION"

    cd ~
    if [ ! -d $VERSION ]; then
        wget --no-verbose -N --referer=http://www.azul.com/downloads/zulu/zulu-linux http://cdn.azul.com/zulu/bin/${VERSION}.tar.gz -O ${VERSION}.tar.gz
        tar xfz ${VERSION}.tar.gz
        echo [INFO] INSTALLING
    fi

    addJavaHome ${VERSION}
}

function addJavaHome {
    TMP_JAVA_HOME=$1

    if grep -q "JAVA_HOME=" ~/.bashrc; then
        echo "Updating Java version to $TMP_JAVA_HOME"
        sed -i'' "s|JAVA_HOME=.*|JAVA_HOME=${TMP_JAVA_HOME}|" ~/.bashrc
    else
        echo "Installing Java version to $TMP_JAVA_HOME"
        prepend 'export PATH=$JAVA_HOME/bin:$PATH' ~/.bashrc
        prepend "export JAVA_HOME=$TMP_JAVA_HOME" ~/.bashrc
    fi
}
