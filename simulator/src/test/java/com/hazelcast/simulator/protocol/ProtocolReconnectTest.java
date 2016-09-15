package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_OPERATION;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getAgentConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getAgentStartPort;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getCoordinatorConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getWorkerConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.shutdownCoordinatorConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_COORDINATOR_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static org.junit.Assert.assertNull;

public class ProtocolReconnectTest {

    private static final Logger LOGGER = Logger.getLogger(ProtocolReconnectTest.class);

    @Before
    public void before() {
        startSimulatorComponents(1, 1, 1);
    }

    @After
    public void after() {
        stopSimulatorComponents();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void reconnect() {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress testAddress = new SimulatorAddress(WORKER, 1, 1, 0);

        // assert that the connection is working downstream
        Response response = sendFromCoordinator(testAddress);
        assertSingleTarget(response, testAddress, SUCCESS);

        // assert that the connection is working upstream
        response = worker.invoke(testAddress, COORDINATOR, DEFAULT_OPERATION);
        assertSingleTarget(response, testAddress, COORDINATOR, SUCCESS);

        // shutdown connection
        shutdownCoordinatorConnector();
        assertNull(getCoordinatorConnector());

        // assert that there are no connections found anymore
        response = worker.invoke(testAddress, COORDINATOR, DEFAULT_OPERATION);
        assertSingleTarget(response, testAddress, getAgentConnector(0).getAddress(), FAILURE_COORDINATOR_NOT_FOUND);

        LOGGER.info("--------------------------");
        LOGGER.info("Starting new connection...");
        LOGGER.info("--------------------------");

        CoordinatorConnector newConnector = startCoordinator("127.0.0.1", getAgentStartPort(), 1);

        // assert that new connection is working downstream
        response = newConnector.invoke(testAddress, DEFAULT_OPERATION);
        assertSingleTarget(response, testAddress, SUCCESS);

        // assert that the new connection is working upstream
        response = worker.invoke(testAddress, COORDINATOR, DEFAULT_OPERATION);
        assertSingleTarget(response, testAddress, COORDINATOR, SUCCESS);

        newConnector.close();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void connectTwice() {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress workerAddress = new SimulatorAddress(WORKER, 1, 1, 0);

        // assert that the connection is working downstream
        Response response = sendFromCoordinator(workerAddress);
        assertSingleTarget(response, workerAddress, SUCCESS);

        // assert that the connection is working upstream
        response = worker.invoke(workerAddress, COORDINATOR, DEFAULT_OPERATION);
        assertSingleTarget(response, workerAddress, COORDINATOR, SUCCESS);

        LOGGER.info("-------------------------------");
        LOGGER.info("Starting second connection...");
        LOGGER.info("-------------------------------");

        CoordinatorConnector secondConnector = startCoordinator("127.0.0.1", getAgentStartPort(), 1);

        // assert that first connection is still working downstream
        response = sendFromCoordinator(workerAddress);
        assertSingleTarget(response, workerAddress, SUCCESS);

        // assert that second connection is working downstream
        response = secondConnector.invoke(workerAddress, DEFAULT_OPERATION);
        assertSingleTarget(response, workerAddress, SUCCESS);

        // assert that the connections are working upstream
        response = worker.invoke(workerAddress, COORDINATOR, DEFAULT_OPERATION);
        assertSingleTarget(response, workerAddress, COORDINATOR, SUCCESS);

        // shutdown first connection
        shutdownCoordinatorConnector();

        // assert that the connections are working upstream
        response = worker.invoke(workerAddress, COORDINATOR, DEFAULT_OPERATION);
        assertSingleTarget(response, workerAddress, COORDINATOR, SUCCESS);

        // shutdown second connection
        secondConnector.close();

        // assert that there are no connections found anymore
        response = worker.invoke(workerAddress, COORDINATOR, DEFAULT_OPERATION);
        assertSingleTarget(response, workerAddress, getAgentConnector(0).getAddress(), FAILURE_COORDINATOR_NOT_FOUND);
    }
}
