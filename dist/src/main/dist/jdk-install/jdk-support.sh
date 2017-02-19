#!/bin/sh

function install {
        PACKAGE=$1

        if hash ${PACKAGE} 2>/dev/null; then
            echo ${PACKAGE} already installed
            return 0
        fi

        if hash apt-get 2>/dev/null; then
            sudo apt-get update
            sudo apt-get install -y ${PACKAGE}
            echo apt-get available
        elif hash yum 2>/dev/null; then
            echo yum available
            sudo yum -y install ${PACKAGE}
        else
            echo apt-get not available
        fi
}

function prepend {
    echo $1|cat - $2 > /tmp/out && mv /tmp/out $2
}
