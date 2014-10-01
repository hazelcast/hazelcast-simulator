#!/bin/sh

max=1
everySeconds=1

ips=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'


for (( i=1; i<=$max; i++ ))
do

    for box in $ips
    do
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "rm -f serverStackTrace.txt; jps | grep Member.* | cut -d ' ' -f1 | xargs -L 1  jstack $1 >> serverStackTrace.txt"
        ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "rm -f clientStackTrace.txt; jps | grep Client.* | cut -d ' ' -f1 | xargs -L 1  jstack $1 >> clientStackTrace.txt"

        scp stabilizer@${box}:serverStackTrace.txt ${box}serverStackTrace.txt
        scp stabilizer@${box}:clientStackTrace.txt ${box}clientStackTrace.txt
    done

    for box in $ips
    do
        cat ${box}serverStackTrace.txt >> serverStackTrace${i}.txt
        cat ${box}clientStackTrace.txt >> clientStackTrace${i}.txt

        rm -f ${box}serverStackTrace.txt
        rm -f ${box}clientStackTrace.txt
    done

    sleep ${everySeconds}

done

echo "The End"
