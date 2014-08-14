#!/bin/bash

for i in {1..10}
do
   echo "run $i"
   rm -f nohup.out
   nohup ./run.sh > run${i}.out 2>&1 &
   ./monkeyListener.sh run${i}.out
done