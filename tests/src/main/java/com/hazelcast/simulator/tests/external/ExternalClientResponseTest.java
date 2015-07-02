package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class ExternalClientResponseTest {

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientResponseTest.class);

    // properties
    public String basename = "externalClientsFinished";
    public int delaySeconds = 60;

    private HazelcastInstance hazelcastInstance;
    private ICountDownLatch clientsFinished;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        hazelcastInstance = testContext.getTargetInstance();
        clientsFinished = hazelcastInstance.getCountDownLatch(basename);
    }

    @Run
    public void run() {
        if (isMemberNode(hazelcastInstance)) {
            return;
        }

        sleepSeconds(delaySeconds);
        clientsFinished.countDown();
        LOGGER.info("Client response sent!");
    }
}
