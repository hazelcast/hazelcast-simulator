package com.hazelcast.simulator.common.messaging;

import java.util.Random;

@MessageSpec(value = "spinCore", description = "triggers a thread spinning indefinitely")
public class SpinCoreIndefinitelyMessage extends RunnableMessage {
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
            new MyThread().start();
        }
    }

    private static class MyThread extends Thread {
        @Override
        public void run() {
            Random random = new Random();
            for (; ; ) {
                if (random.nextInt(100) == 101) {
                    System.out.println("Can't happen");
                }
            }
        }
    }
}

