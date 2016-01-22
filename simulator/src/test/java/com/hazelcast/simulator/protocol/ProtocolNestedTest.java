package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertAllTargets;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Operation.NESTED_ASYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Operation.NESTED_SYNC;

public class ProtocolNestedTest {

    private static final int CONCURRENCY_LEVEL = 500;

    private static final SimulatorAddress ALL_WORKERS = new SimulatorAddress(WORKER, 0, 0, 0);
    private static final SimulatorAddress ALL_TESTS = new SimulatorAddress(TEST, 0, 0, 0);

    private static final IntegrationTestOperation NESTED_SYNC_OPERATION = new IntegrationTestOperation(null, NESTED_SYNC);
    private static final IntegrationTestOperation NESTED_ASYNC_OPERATION = new IntegrationTestOperation(null, NESTED_ASYNC);

    private static final Logger LOGGER = Logger.getLogger(ProtocolNestedTest.class);

    @Before
    public void setUp() {
        setLogLevel(Level.TRACE);

        startSimulatorComponents(1, 2, 2);
    }

    @After
    public void tearDown() {
        stopSimulatorComponents();

        resetLogLevel();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toWorker_syncWrite() {
        run(ALL_WORKERS, NESTED_SYNC_OPERATION, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toWorker_syncWrite_concurrently() {
        runConcurrently("nestedMessage_toWorker_syncWrite_concurrently", ALL_WORKERS, NESTED_SYNC_OPERATION, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toWorker_asyncWrite() {
        run(ALL_WORKERS, NESTED_ASYNC_OPERATION, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toWorker_asyncWrite_concurrently() {
        runConcurrently("nestedMessage_toWorker_asyncWrite_concurrently", ALL_WORKERS, NESTED_ASYNC_OPERATION, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toTest_syncWrite() {
        run(ALL_TESTS, NESTED_SYNC_OPERATION, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toTest_syncWrite_concurrently() {
        runConcurrently("nestedMessage_toTest_syncWrite_concurrently", ALL_TESTS, NESTED_SYNC_OPERATION, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toTest_asyncWrite() {
        run(ALL_TESTS, NESTED_ASYNC_OPERATION, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_toTest_asyncWrite_concurrently() {
        runConcurrently("nestedMessage_toTest_asyncWrite_concurrently", ALL_TESTS, NESTED_ASYNC_OPERATION, 4);
    }

    private static void run(SimulatorAddress target, SimulatorOperation operation, int expectedResponseCount) {
        Response response = sendFromCoordinator(target, operation);

        LOGGER.info("Response: " + response);
        assertAllTargets(response, target, SUCCESS, expectedResponseCount);
    }

    private static void runConcurrently(String spawnerName, final SimulatorAddress target, final SimulatorOperation operation,
                                        final int expectedResponseCount) {
        ThreadSpawner spawner = new ThreadSpawner(spawnerName, true);
        for (int i = 0; i < CONCURRENCY_LEVEL; i++) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    Response response = sendFromCoordinator(target, operation);

                    LOGGER.info("Response: " + response);
                    assertAllTargets(response, target, SUCCESS, expectedResponseCount);
                }
            });
        }
        spawner.awaitCompletion();
    }
}
