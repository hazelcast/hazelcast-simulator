#!/bin/sh

list=$(cat agents.txt | cut -d',' -f1)

readarray -t array <<<"$list"

box=${array[2]}

ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "jps | grep Worker | cut -d ' ' -f1 | xargs -L 1  kill -9 $1"
