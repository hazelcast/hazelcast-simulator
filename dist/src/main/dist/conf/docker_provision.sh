#!/bin/bash

# exit on failure
set -e
count=$1
container_image=jerrinot/ubuntu-with-ssh:latest
# TODO: make the container image configurable via Simulator.properties

 # Check if Docker is installed.
if ! hash docker 2>/dev/null ; then
    echo "[ERROR]Docker is not installed!"
    exit 1
fi

# TODO: check non-root user can execute docker
# this is not default on Linux


# TODO: Check records in agents.txt match running Docker images

n=0
while [ "$n" -lt $count ]; do
    n=$(( n + 1 ))

    random=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1)
    docker run -d -l simulator=${random} -p 22 -p 9000 "$container_image"
    id=$(docker ps --filter "label=simulator=$random" --format "{{.ID}}")
    ssh=$(docker inspect --format='{{(index (index .NetworkSettings.Ports "22/tcp") 0).HostPort}}' $id)
    #TODO: check it returns an expected value
    #echo "ssh port: ssh"
    broker=$(docker inspect --format='{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' $id)
    #TODO: check it returns an expected value
    #echo "broker port: $broker"
    ip=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $id)
    echo "127.0.0.1,$ip|ssh=$ssh,broker=$broker" >> agents.txt
done