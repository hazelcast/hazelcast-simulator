package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class ExternalClientTest {

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientTest.class);

    // properties
    public String basename = "externalClientsFinished";
    public int waitForClientsCount = 1;
    public int waitIntervalSeconds = 60;

    private ICountDownLatch clientsFinished;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        this.clientsFinished = hazelcastInstance.getCountDownLatch(basename);

        clientsFinished.trySetCount(waitForClientsCount);
    }

    @Run
    public void run() {
        long finishedClients;
        do {
            sleepSeconds(waitIntervalSeconds);
            try {
                clientsFinished.await(waitIntervalSeconds, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                EmptyStatement.ignore(ignored);
            }
            finishedClients = clientsFinished.getCount();
        } while (finishedClients < waitForClientsCount);
        LOGGER.info("Got response from " + finishedClients + " clients, stopping now!");
    }
}
