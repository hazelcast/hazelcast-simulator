#!/bin/sh

runid=$1

ips=$(cat agents.txt | cut -d',' -f1)

IFS=$'\n'

interval=5
itter=10

for box in $ips
do
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "killall -9 mpstat"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "killall -9 sar"
     ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${box} "killall -9 pidstat"

     scp stabilizer@${box}:mpstats.txt ${runid}_${box}_mpstats.out
     scp stabilizer@${box}:vmstats.txt ${runid}_${box}_vmstats.out
     scp stabilizer@${box}:runQ.txt ${runid}_${box}_runQ.out
     scp stabilizer@${box}:net.txt ${runid}_${box}_net.out
     scp stabilizer@${box}:errorNet.txt ${runid}_${box}_errorNet.out
     scp stabilizer@${box}:sock.txt ${runid}_${box}_sock.out
     scp stabilizer@${box}:pidStatsOverView.txt ${runid}_${box}_pidStatsOverView.out
     scp stabilizer@${box}:pidStatsMem.txt ${runid}_${box}_pidStatsMem.out
     scp stabilizer@${box}:pidStatsIO.txt ${runid}_${box}_pidStatsIO.out
     scp stabilizer@${box}:pidStatsSwitch.txt ${runid}_${box}pidStatsSwitch.out

done