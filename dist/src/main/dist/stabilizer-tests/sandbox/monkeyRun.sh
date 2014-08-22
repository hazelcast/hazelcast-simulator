#!/bin/bash

for i in {1..1}
do
   echo "run $i"

   name=nohupRun${i}.out

   nohup ./run.sh > ${name} 2>&1 &

   ./monkeyListener.sh ${name} "Running 00d 00h 01m 00s"

   ./unblockIp.sh

done
