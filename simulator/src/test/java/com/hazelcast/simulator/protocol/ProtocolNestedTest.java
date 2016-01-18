package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertAllTargets;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Operation.NESTED_ASYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Operation.NESTED_SYNC;

public class ProtocolNestedTest {

    private static final SimulatorAddress ALL_WORKERS = new SimulatorAddress(WORKER, 0, 0, 0);

    private static final Logger LOGGER = Logger.getLogger(ProtocolNestedTest.class);

    @Before
    public void setUp() {
        setLogLevel(Level.TRACE);

        startSimulatorComponents(1, 2, 1);
    }

    @After
    public void tearDown() {
        stopSimulatorComponents();

        resetLogLevel();
    }

    @Ignore
    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_syncWrite() {
        IntegrationTestOperation operation = new IntegrationTestOperation(null, NESTED_SYNC);

        // assert that the connection is working downstream
        Response response = sendFromCoordinator(ALL_WORKERS, operation);
        LOGGER.info("Response: " + response);
        assertAllTargets(response, ALL_WORKERS, SUCCESS, 2);
    }

    @Ignore
    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void nestedMessage_asyncWrite() {
        IntegrationTestOperation operation = new IntegrationTestOperation(null, NESTED_ASYNC);

        // assert that the connection is working downstream
        Response response = sendFromCoordinator(ALL_WORKERS, operation);
        LOGGER.info("Response: " + response);
        assertAllTargets(response, ALL_WORKERS, SUCCESS, 2);
    }
}
