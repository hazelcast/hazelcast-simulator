package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.ProtocolUtil.AGENT_START_PORT;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_OPERATION;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getCoordinatorConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.shutdownCoordinatorConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static org.junit.Assert.assertNull;

public class ProtocolReconnectTest {

    private static final Logger LOGGER = Logger.getLogger(ProtocolReconnectTest.class);

    @Before
    public void setUp() {
        setLogLevel(Level.TRACE);

        startSimulatorComponents(1, 0, 0);
    }

    @After
    public void tearDown() {
        stopSimulatorComponents();

        resetLogLevel();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void connectTwice() {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 1, 0, 0);

        Response response = sendFromCoordinator(destination);
        assertSingleTarget(response, destination, SUCCESS);

        LOGGER.info("-------------------------------");
        LOGGER.info("Starting second connection...");
        LOGGER.info("-------------------------------");

        CoordinatorConnector secondConnector = null;
        try {
            secondConnector = startCoordinator("127.0.0.1", AGENT_START_PORT, 1);

            // assert that first connection is still working
            response = sendFromCoordinator(destination);
            assertSingleTarget(response, destination, SUCCESS);

            // assert that second connection is working
            response = secondConnector.write(destination, DEFAULT_OPERATION);
            assertSingleTarget(response, destination, SUCCESS);
        } finally {
            if (secondConnector != null) {
                secondConnector.shutdown();
            }
        }
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void reconnect() {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 1, 0, 0);

        Response response = sendFromCoordinator(destination);
        assertSingleTarget(response, destination, SUCCESS);

        shutdownCoordinatorConnector();
        assertNull(getCoordinatorConnector());

        LOGGER.info("--------------------------");
        LOGGER.info("Starting new connection...");
        LOGGER.info("--------------------------");

        CoordinatorConnector newConnector = null;
        try {
            newConnector = startCoordinator("127.0.0.1", AGENT_START_PORT, 1);

            // assert that new connection is working
            response = newConnector.write(destination, DEFAULT_OPERATION);
            assertSingleTarget(response, destination, SUCCESS);
        } finally {
            if (newConnector != null) {
                newConnector.shutdown();
            }
        }
    }
}
