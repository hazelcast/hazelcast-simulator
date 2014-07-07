package com.hazelcast.stabilizer.common.messaging;

@MessageSpec("spinCore")
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
            new Thread() {
                @Override
                public void run() {
                    for (; ; ) {
                    }
                }
            }.start();
        }
    }
}
