#!/bin/sh

if [ $# -eq 0 ]; then
    echo "No profile dir provided"
    exit 1
fi

HOME=$(pwd)

TARGET=$1
DIRS=$(find ${TARGET} -name run.sh | xargs -L 1 dirname)

IFS=$'\n'

for DIR in ${DIRS}
do
	echo "Running stabilizer in folder: ${DIR}"
	cd ${DIR}
	pwd

	rm -f nohup.out
	nohup ./run.sh &
	cd ${HOME}

	sleep 30
done
