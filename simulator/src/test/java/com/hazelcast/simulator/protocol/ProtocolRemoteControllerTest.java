package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.connector.CoordinatorRemoteConnector;
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
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.REMOTE;

public class ProtocolRemoteControllerTest {

    private static final int COORDINATOR_PORT = 11111;

    private static CoordinatorConnector coordinatorConnector;
    private static CoordinatorRemoteConnector coordinatorRemoteConnector;

    @BeforeClass
    public static void beforeClass() {
        setLogLevel(Level.TRACE);

        coordinatorConnector = startCoordinator("127.0.0.1", getAgentStartPort(), 0, COORDINATOR_PORT);

        coordinatorRemoteConnector = new CoordinatorRemoteConnector("127.0.0.1", COORDINATOR_PORT);
        coordinatorRemoteConnector.start();
    }

    @AfterClass
    public static void afterClass() {
        coordinatorRemoteConnector.close();
        coordinatorConnector.shutdown();

        resetLogLevel();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_sendMessageToCoordinator() {
        Response response = coordinatorRemoteConnector.write(new IntegrationTestOperation());

        assertSingleTarget(response, REMOTE, COORDINATOR, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_sendMessageToRemoteController() {
        Response response = coordinatorConnector.writeToRemoteController(new IntegrationTestOperation());

        assertSingleTarget(response, COORDINATOR, REMOTE, SUCCESS);
    }
}
