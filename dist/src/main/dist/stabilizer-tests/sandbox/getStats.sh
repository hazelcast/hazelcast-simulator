#!/bin/sh

runid=$1

ips=$(cat agents.txt | cut -d',' -f1)

IFS=$'\n'

interval=5
itter=10

for box in $ips
do
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "killall -9 mpstat"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "killall -9 sar"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no simulator@${box} "killall -9 pidstat"

     scp simulator@${box}:mpstats.txt ${runid}_${box}_mpstats.out
     scp simulator@${box}:vmstats.txt ${runid}_${box}_vmstats.out
     scp simulator@${box}:runQ.txt ${runid}_${box}_runQ.out
     scp simulator@${box}:net.txt ${runid}_${box}_net.out
     scp simulator@${box}:errorNet.txt ${runid}_${box}_errorNet.out
     scp simulator@${box}:sock.txt ${runid}_${box}_sock.out
     scp simulator@${box}:pidStatsOverView.txt ${runid}_${box}_pidStatsOverView.out
     scp simulator@${box}:pidStatsMem.txt ${runid}_${box}_pidStatsMem.out
     scp simulator@${box}:pidStatsIO.txt ${runid}_${box}_pidStatsIO.out
     scp simulator@${box}:pidStatsSwitch.txt ${runid}_${box}pidStatsSwitch.out

done