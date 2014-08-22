#!/bin/sh

list=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'

box=${list[2]}

scp block.sh stabilizer@${box}:~
ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "chmod +x block.sh; ./block.sh eth0"


