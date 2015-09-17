package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.ProtocolUtil.AGENT_START_PORT;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_OPERATION;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;

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

    @Ignore
    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void connectTwice() {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 1, 0, 0);

        Response response = sendFromCoordinator(destination);
        assertSingleTarget(response, destination, SUCCESS);

        LOGGER.info("-------------------------------");
        LOGGER.info("Starting second connection...");
        LOGGER.info("-------------------------------");

        CoordinatorConnector connector = null;
        try {
            connector = startCoordinator("127.0.0.1", AGENT_START_PORT, 1);

            response = connector.write(destination, DEFAULT_OPERATION);
            assertSingleTarget(response, destination, SUCCESS);
        } finally {
            if (connector != null) {
                connector.shutdown();
            }
        }
    }
}
