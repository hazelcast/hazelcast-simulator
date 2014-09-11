#!/bin/sh

list=$(cat agents.txt | cut -d',' -f1)

#Set the field separator to new line
IFS=$'\n'

for item in $list
do
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no stabilizer@${item} "rm -f jstackTrack.txt; jps | grep .*Worker | cut -d ' ' -f1 | xargs -L 1  jstack $1 >> jstackTrack.txt"
	scp stabilizer@${item}:jstackTrack.txt ${item}temp
done

for item in $list
do
  cat ${item}temp >> jstackTrack.txt
  rm -f ${item}temp
done
