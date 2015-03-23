package com.hazelcast.simulator.common.messaging;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@MessageSpec(value = "oom", description = "starts a new thread allocating memory in JVM heap indefinitely")
public class UseAllMemoryMessage extends RunnableMessage {
    private static final Logger LOGGER = Logger.getLogger(UseAllMemoryMessage.class);

    private static List list = new ArrayList();

    private final int bufferSize = 1000;
    private final int delay;

    public UseAllMemoryMessage(MessageAddress messageAddress, int delay) {
        super(messageAddress);
        this.delay = delay;
    }

    public UseAllMemoryMessage(MessageAddress messageAddress) {
        this(messageAddress, 0);
    }

    @Override
    public boolean disableMemberFailureDetection() {
        return super.disableMemberFailureDetection();
    }

    @Override
    public boolean removeFromAgentList() {
        return super.removeFromAgentList();
    }

    @Override
    public void run() {
        Thread thread = new Thread() {
            @Override
            public void run() {
                LOGGER.debug("Starting a thread to consume all memory");
                for (; ; ) {
                    try {
                        allocateMemory();
                    } catch (OutOfMemoryError ex) {
                        //ignore
                    }
                }
            }

            private void allocateMemory() {
                while (!interrupted()) {
                    byte[] buff = new byte[bufferSize];
                    list.add(buff);
                    sleepMillisInterruptThread(delay);
                }
            }

            private void sleepMillisInterruptThread(int sleepMillis) {
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepMillis);
                } catch (InterruptedException e) {
                    LOGGER.warn("Interrupted during sleep.");
                    Thread.currentThread().interrupt();
                }
            }
        };
        thread.start();
    }
}
