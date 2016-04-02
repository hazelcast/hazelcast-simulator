package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.CommunicatorConnector;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetLogLevel;
import static com.hazelcast.simulator.TestEnvironmentUtils.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getAgentStartPort;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startCoordinator;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.REMOTE;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;

public class ProtocolCommunicatorTest {

    private static final int COORDINATOR_PORT = 11111;

    private static CoordinatorConnector coordinatorConnector;
    private static CommunicatorConnector communicatorConnector;

    @BeforeClass
    public static void setUp() {
        setLogLevel(Level.TRACE);

        coordinatorConnector = startCoordinator("127.0.0.1", getAgentStartPort(), 0, COORDINATOR_PORT);

        communicatorConnector = new CommunicatorConnector("127.0.0.1", COORDINATOR_PORT);
        communicatorConnector.start();
    }

    @AfterClass
    public static void tearDown() {
        communicatorConnector.shutdown();
        coordinatorConnector.shutdown();

        resetLogLevel();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_sendMessageToCoordinator() {
        Response response = communicatorConnector.write(new IntegrationTestOperation());

        assertSingleTarget(response, REMOTE, COORDINATOR, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_sendMessageToCommunicator() {
        Response response = coordinatorConnector.writeToCommunicator(new IntegrationTestOperation());

        assertSingleTarget(response, COORDINATOR, REMOTE, SUCCESS);
    }
}
