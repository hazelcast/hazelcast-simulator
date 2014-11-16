#!/bin/bash

targetFile=$1
actionTriger=$2

fifo=tempNotifyFifo
mkfifo "${fifo}" || exit 1
{
    # run tail in the background so that the shell can
    # kill tail when notified that grep has exited
    tail -f ${targetFile} &
    # remember tail's PID
    tailpid=$!
    # wait for notification that grep has exited
    read runid <${fifo}
    # grep has exited, time to go
    kill "${tailpid}"

    mv ${targetFile} nohup-${runid}.out

} | {

    id=$(grep -oh -m 1 "Starting testsuite: [0-9].*" | cut -d ' ' -f3)

    ./startStats.sh

    grep -m 1 "${actionTriger}"

    sh ./monkeyAction.sh &

    grep -m 1 "The End"

    ./getStats.sh "${id}"

    # notify the first pipeline stage that grep is done
    echo "${id}">${fifo}
}
# clean up
rm "${fifo}"

