#!/bin/sh

ips=$(cat agents.txt | cut -d',' -f1)

IFS=$'\n'

for box in $ips
do
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "sudo yum -y install sysstat"
done