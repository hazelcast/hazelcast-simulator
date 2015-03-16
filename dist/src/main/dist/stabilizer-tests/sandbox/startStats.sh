#!/bin/sh

ips=$(cat agents.txt | cut -d',' -f1)

IFS=$'\n'

interval=10

for box in $ips
do
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "mpstat -P ALL $interval > mpstats.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "vmstat $interval > vmstats.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "sar -n DEV $interval > net.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "sar -q $interval > runQ.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "sar -n EDEV $interval > errorNet.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "sar -n SOCK $interval > sock.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "pidstat -C java $interval > pidStatsOverView.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "pidstat -C java -r $interval > pidStatsMem.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "pidstat -C java -d $interval > pidStatsIO.txt &"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "pidstat -C java -w -I -t $interval > pidStatsSwitch.txt &"
done
