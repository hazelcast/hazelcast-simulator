#!/bin/sh

list=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'

for item in $list
do
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${item} "sudo iptables -F"
	break
done
