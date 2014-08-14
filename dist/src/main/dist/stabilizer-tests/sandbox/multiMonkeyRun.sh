#!/bin/bash

for i in {1..10}
do
   echo "run $i"

   name=nohupRun${i}.out

   nohup ./run.sh > ${name} 2>&1 &

   ./monkeyListener.sh ${name}
done