#!/bin/sh

ips=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'

s3cp=$(find ~ -name "s3c*.jar")

for box in $ips
do
    scp $s3cp simulator@${box}:~
    scp -r ~/.s3cp simulator@${box}:~

    ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "find . -name "*.hprof" | xargs -I % sh -c 'echo %; java -jar s3cp-cmdline-0.1.11.jar % s3://simulator/tmp/' "
done
