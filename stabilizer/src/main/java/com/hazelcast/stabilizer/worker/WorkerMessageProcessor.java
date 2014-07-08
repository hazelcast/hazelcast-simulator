package com.hazelcast.stabilizer.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
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

    private HazelcastInstance hazelcastServerInstance;
    private HazelcastInstance hazelcastClientInstance;

    public WorkerMessageProcessor(ConcurrentMap<String, TestContainer<TestContext>> tests) {
        this.tests = tests;
    }

    public void setHazelcastServerInstance(HazelcastInstance hazelcastServerInstance) {
        this.hazelcastServerInstance = hazelcastServerInstance;
    }

    public void setHazelcastClientInstance(HazelcastInstance hazelcastClientInstance) {
        this.hazelcastClientInstance = hazelcastClientInstance;
    }

    public void processMessage(Message message) {
        injectHazecastInstance(message);

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

    private void injectHazecastInstance(Message message) {
        if (message instanceof HazelcastInstanceAware) {
            if (hazelcastServerInstance != null) {
                ((HazelcastInstanceAware) message).setHazelcastInstance(hazelcastServerInstance);
            } else if (hazelcastClientInstance != null) {
                ((HazelcastInstanceAware) message).setHazelcastInstance(hazelcastClientInstance);
            } else {
                log.warning("Message "+message.getClass().getName()+" implements "
                        +HazelcastInstanceAware.class+" interface, but no instance is currently running in this worker.");
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
        log.info("Processing local runnable message: "+message.getClass().getName());
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
