#!/bin/sh

if [ $# -eq 0 ]; then
    echo "No profile dir provided"
    exit 1
fi

target=$1

find ${target} -name "nohup.out" | xargs -I % sh -c 'echo %; cat % | grep -i "Provisioning\|provisioned\|Finished terminating\|Failure #1 \|failures have been detected"; echo"" '