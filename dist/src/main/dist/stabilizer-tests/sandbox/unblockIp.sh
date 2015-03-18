#!/bin/sh

list=$(cat agents.txt | cut -d',' -f1)

readarray -t array <<<"$list"

box=${array[2]}

echo "${box}"

ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "sudo /sbin/iptables -F"
