package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import com.hazelcast.simulator.test.TestContext;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.simulator.utils.ExecutorFactory.createScheduledThreadPool;
import static com.hazelcast.simulator.utils.HazelcastUtils.injectHazelcastInstance;
import static com.hazelcast.simulator.utils.HazelcastUtils.isMaster;

/**
 * Processes {@link Message} instances on {@link MemberWorker} and {@link ClientWorker} instances.
 */
class WorkerMessageProcessor {

    private static final int DELAY_SECONDS = 10;

    private static final Logger LOGGER = Logger.getLogger(WorkerMessageProcessor.class);

    private final ScheduledExecutorService executor = createScheduledThreadPool(10, WorkerMessageProcessor.class);
    private final Random random = new Random();

    private final ConcurrentMap<String, TestContainer<TestContext>> tests;
    private final HazelcastInstance hazelcastInstance;

    WorkerMessageProcessor(ConcurrentMap<String, TestContainer<TestContext>> tests, HazelcastInstance hazelcastInstance) {
        this.tests = tests;
        this.hazelcastInstance = hazelcastInstance;
    }

    void submit(final Message message) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if (shouldProcess(message)) {
                    process(message);
                }
            }
        });
    }

    private boolean shouldProcess(Message message) {
        String workerAddress = message.getMessageAddress().getWorkerAddress();
        if (workerAddress.equals(MessageAddress.WORKER_WITH_OLDEST_MEMBER)) {
            return isMaster(hazelcastInstance, executor, DELAY_SECONDS);
        }
        return true;
    }

    private void process(Message message) {
        injectHazelcastInstance(hazelcastInstance, message);

        if (message.getMessageAddress().getTestAddress() == null) {
            processLocalMessage(message);
        } else {
            try {
                processTestMessage(message);
            } catch (Throwable throwable) {
                LOGGER.fatal("Error while processing message", throwable);
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
        LOGGER.info("Processing local runnable message: " + message.getClass().getName());
        message.run();
    }

    private void processTestMessage(Message message) throws Exception {
        String testAddress = message.getMessageAddress().getTestAddress();
        if (MessageAddress.BROADCAST.equals(testAddress)) {
            for (TestContainer<TestContext> testContainer : tests.values()) {
                testContainer.sendMessage(message);
            }
        } else if (MessageAddress.RANDOM.equals(testAddress)) {
            TestContainer<?> randomTestContainer = getRandomTestContainerOrNull();
            if (randomTestContainer == null) {
                LOGGER.warn("No test container is known to this worker. Is it a race-condition?");
            } else {
                randomTestContainer.sendMessage(message);
            }
        }
    }

    private TestContainer<?> getRandomTestContainerOrNull() {
        Collection<TestContainer<TestContext>> testContainers = tests.values();
        int size = testContainers.size();
        if (size == 0) {
            return null;
        }

        TestContainer<?>[] testContainerArray = testContainers.toArray(new TestContainer<?>[size]);
        return testContainerArray[random.nextInt(size)];
    }
}
