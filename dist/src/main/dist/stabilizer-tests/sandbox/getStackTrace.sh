#!/bin/sh

everySeconds=10
max=12

ips=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'


for i in {1..${max}}
do

    for box in $ips
    do
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "rm -f jstackTrack.txt; jps | grep .*Worker | cut -d ' ' -f1 | xargs -L 1  jstack $1 >> jstackTrack.txt"
        scp stabilizer@${box}:jstackTrack.txt ${box}temp
    done

    for box in $ips
    do
        cat ${box}temp >> jstackTrack${i}.txt
        rm -f ${box}temp
    done

    sleep ${everySeconds}

done

echo "The End"

