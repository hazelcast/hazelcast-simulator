package com.hazelcast.simulator.utils.helper;

import static com.hazelcast.simulator.utils.CommonUtils.sleepNanos;

public final class CallerInterrupter extends Thread {

    private final Thread callerThread;
    private final long sleepNanos;

    public CallerInterrupter(Thread callerThread, long sleepNanos) {
        this.callerThread = callerThread;
        this.sleepNanos = sleepNanos;
    }

    @Override
    public void run() {
        sleepNanos(sleepNanos);
        callerThread.interrupt();
    }
}
