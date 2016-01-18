package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertAllTargets;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getCoordinatorConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Operation.NESTED_ASYNC;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Operation.NESTED_SYNC;

public class ProtocolNestedTest {

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

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    @Ignore
    public void nestedMessage_syncWrite() {
        SimulatorAddress allWorkers = new SimulatorAddress(WORKER, 0, 0, 0);

        getCoordinatorConnector().write(allWorkers, new IntegrationTestOperation(null, NESTED_SYNC));

        // assert that the connection is working downstream
        Response response = sendFromCoordinator(allWorkers);
        assertAllTargets(response, allWorkers, SUCCESS, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    @Ignore
    public void nestedMessage_asyncWrite() {
        SimulatorAddress allWorkers = new SimulatorAddress(WORKER, 0, 0, 0);

        getCoordinatorConnector().write(allWorkers, new IntegrationTestOperation(null, NESTED_ASYNC));

        // assert that the connection is working downstream
        Response response = sendFromCoordinator(allWorkers);
        assertAllTargets(response, allWorkers, SUCCESS, 2);
    }
}
