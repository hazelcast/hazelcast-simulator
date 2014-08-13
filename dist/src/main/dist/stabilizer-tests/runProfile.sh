#!/bin/sh

if [ $# -eq 0 ]; then
    echo "No profile dir provided"
    exit 1
fi

target=$1

dirs=$(find ${target} -name run.sh | xargs -L 1 dirname)

IFS=$'\n'

home=$(pwd)

for dir in $dirs
do
	cd ${dir}
	pwd

	rm -f nohup.out
	nohup ./run.sh &
	cd ${home}

	sleep 15
done