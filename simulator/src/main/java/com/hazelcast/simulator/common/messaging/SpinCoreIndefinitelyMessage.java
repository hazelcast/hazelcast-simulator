package com.hazelcast.simulator.common.messaging;

import org.apache.log4j.Logger;

import java.util.Random;

@MessageSpec(value = "spinCore", description = "Triggers a number of threads who are spinning indefinitely.")
public class SpinCoreIndefinitelyMessage extends RunnableMessage {

    private static final Logger LOGGER = Logger.getLogger(SoftKillMessage.class);

    private final int noOfThreads;

    public SpinCoreIndefinitelyMessage(MessageAddress messageAddress) {
        this(messageAddress, 1);
    }

    public SpinCoreIndefinitelyMessage(MessageAddress messageAddress, int noOfThread) {
        super(messageAddress);
        this.noOfThreads = noOfThread;
    }

    @Override
    public void run() {
        for (int i = 0; i < noOfThreads; i++) {
            new BusySpinner().start();
        }
    }

    private static class BusySpinner extends Thread {

        private final Random random = new Random();

        @Override
        @SuppressWarnings("checkstyle:magicnumber")
        public void run() {
            while (!interrupted()) {
                if (random.nextInt(100) == 101) {
                    LOGGER.fatal("Can't happen!");
                }
            }
        }
    }
}
