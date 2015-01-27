package com.hazelcast.stabilizer.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.common.messaging.MessageAddress;
import com.hazelcast.stabilizer.test.TestContext;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class WorkerMessageProcessor {
    private static final Logger log = Logger.getLogger(WorkerMessageProcessor.class);
    private static final int TIMEOUT = 60;

    private final ConcurrentMap<String, TestContainer<TestContext>> tests;
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

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

    public void submit(final Message message) {
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if (shouldProcess(message)) {
                    process(message);
                }
            }
        });
    }

    private void process(Message message) {
        injectHazecastInstance(message);
        if (message.getMessageAddress().getTestAddress() == null) {
            processLocalMessage(message);
        } else {
            try {
                processTestMessage(message);
            } catch (Throwable throwable) {
                log.fatal("Error while processing message", throwable);
            }
        }
    }

    private boolean shouldProcess(Message message) {
        String workerAddress = message.getMessageAddress().getWorkerAddress();
        if (workerAddress.equals(MessageAddress.WORKER_WITH_OLDEST_MEMBER)) {
            return isMaster();
        } else {
            return true;
        }
    }

    private boolean isMaster() { //TODO: This should be really factored out
        if (hazelcastServerInstance == null || !isOldestMember()) {
            return false;
        }
        try {
            return executor.schedule(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return isOldestMember();
                }
            }, 10, TimeUnit.SECONDS).get(TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    private boolean isOldestMember() {
        Iterator<Member> memberIterator = hazelcastServerInstance.getCluster().getMembers().iterator();
        boolean master = memberIterator.hasNext() && memberIterator.next().equals(hazelcastServerInstance.getLocalEndpoint());
        return master;
    }

    private void injectHazecastInstance(Message message) {
        if (message instanceof HazelcastInstanceAware) {
            if (hazelcastServerInstance != null) {
                ((HazelcastInstanceAware) message).setHazelcastInstance(hazelcastServerInstance);
            } else if (hazelcastClientInstance != null) {
                ((HazelcastInstanceAware) message).setHazelcastInstance(hazelcastClientInstance);
            } else {
                log.warn("Message " + message.getClass().getName() + " implements " + HazelcastInstanceAware.class
                        + " interface, but no instance is currently running in this worker.");
            }
        }
    }

    private void processTestMessage(Message message) throws Throwable {
        String testAddress = message.getMessageAddress().getTestAddress();
        if (MessageAddress.BROADCAST.equals(testAddress)) {
            for (TestContainer<TestContext> testContainer : tests.values()) {
                testContainer.sendMessage(message);
            }
        } else if (MessageAddress.RANDOM.equals(testAddress)) {
            TestContainer<?> randomTestContainer = getRandomTestContainerOrNull();
            if (randomTestContainer == null) {
                log.warn("No test container is known to this worker. Is it a race-condition?");
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
