#!/bin/sh


if [ $# -eq 0 ]; then
    echo "No s3 upload dir provided"
    exit 1
fi

issue=$1

list=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'

s3cp=$(find ~ -name "s3c*.jar")

for item in $list
do
    scp $s3cp stabilizer@${item}:~
    scp -r ~/.s3cp stabilizer@${item}:~
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${item} 'find . -name "*.hprof" | xargs -L 1  java -jar s3cp-cmdline-0.1.11.jar $1 s3://stabilizer/'${issue}'/'$1
done