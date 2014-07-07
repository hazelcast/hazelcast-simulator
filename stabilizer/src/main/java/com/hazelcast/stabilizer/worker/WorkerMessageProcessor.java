package com.hazelcast.stabilizer.worker;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.tests.TestContext;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class WorkerMessageProcessor {
    private static final ILogger log = Logger.getLogger(WorkerMessageProcessor.class);

    private final ConcurrentMap<String, TestContainer<TestContext>> tests;
    private String testAddress;
    private Random random = new Random();

    public WorkerMessageProcessor(ConcurrentMap<String, TestContainer<TestContext>> tests) {
        this.tests = tests;
    }

    public void processMessage(Message message) {
        if (message.getMessageAddress().getTestAddress() == null) {
            processLocalMessage(message);
        } else {
            try {
                processTestMessage(message);
            } catch (Throwable throwable) {
                log.severe("Error while processing message", throwable);
            }
        }
    }

    private void processTestMessage(Message message) throws Throwable {
        testAddress = message.getMessageAddress().getTestAddress();
        if (MessageAddress.BROADCAST_PREFIX.equals(testAddress)) {
            for (TestContainer<TestContext> testContainer : tests.values()) {
                testContainer.sendMessage(message);
            }
        } else if (MessageAddress.RANDOM_PREFIX.equals(testAddress)) {
            TestContainer<?> randomTestContainer = getRandomTestContainerOrNull();
            if (randomTestContainer == null) {
                log.warning("No test container is known to this worker. Is it a race-condition?");
            } else {
                randomTestContainer.sendMessage(message);
            }
        }
    }

    private void processLocalMessage(Message message) {
        if (message instanceof Runnable) {
            processLocalRunnableMessage((Runnable) message);
        } else {
            throw new UnsupportedOperationException("Non-runnable messages to workers are not implemented yet");
        }
    }

    private void processLocalRunnableMessage(Runnable message) {
        Runnable executable = message;
        executable.run();
    }

    public TestContainer<?> getRandomTestContainerOrNull() {
        TestContainer<?>[] testContainers = tests.values().toArray(new TestContainer<?>[]{});
        if (testContainers.length == 0) {
            return null;
        }
        TestContainer<?> randomTestContainer = testContainers[random.nextInt(testContainers.length)];
        return randomTestContainer;
    }
}
