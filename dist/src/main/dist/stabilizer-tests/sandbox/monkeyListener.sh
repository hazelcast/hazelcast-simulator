#!/bin/bash

targetFile=$1

fifo=tempNotifyFifo
mkfifo "${fifo}" || exit 1
{
    # run tail in the background so that the shell can
    # kill tail when notified that grep has exited
    tail -f ${targetFile} &
    # remember tail's PID
    tailpid=$!
    # wait for notification that grep has exited
    read foo <${fifo}
    # grep has exited, time to go
    kill "${tailpid}"
} | {
    grep -m 1 "Running 00d 00h 01m 00s"

    sh ./monkeyAction.sh &

    grep -m 1 "The End"

    # notify the first pipeline stage that grep is done
    echo >${fifo}
}
# clean up
rm "${fifo}"