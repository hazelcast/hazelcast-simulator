#!/bin/sh

list=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'

for box in $list
do
    scp block.sh stabilizer@${box}:~
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "chmod +x block.sh; ./block.sh eth0"
	break
done
