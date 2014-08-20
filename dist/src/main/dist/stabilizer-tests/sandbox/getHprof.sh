#!/bin/sh


if [ $# -eq 0 ]; then
    echo "No s3 upload dir provided"
    exit 1
fi

dir=$1

ips=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'

s3cp=$(find ~ -name "s3c*.jar")

for box in $ips
do
    scp $s3cp stabilizer@${box}:~
    scp -r ~/.s3cp stabilizer@${box}:~

    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${item} "find . -name "*.hprof" | xargs -I % sh -c 'echo %; java -jar s3cp-cmdline-0.1.11.jar % s3://stabilizer/tmp/' "
done