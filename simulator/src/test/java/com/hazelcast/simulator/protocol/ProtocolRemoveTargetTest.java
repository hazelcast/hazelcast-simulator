package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.buildMessage;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getAgentConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getCoordinatorConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getWorkerConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetMessageId;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.setLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;

public class ProtocolRemoveTargetTest {

    private static final int DEFAULT_TEST_TIMEOUT = 5000;

    @BeforeClass
    public static void setUp() {
        setLogLevel(Level.TRACE);

        startSimulatorComponents(1, 1, 1);
    }

    @AfterClass
    public static void tearDown() {
        stopSimulatorComponents();

        resetLogLevel();
        resetMessageId();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test() throws Exception {
        SimulatorAddress testDestination = new SimulatorAddress(TEST, 1, 1, 1);

        Response response = sendMessage(testDestination);
        assertSingleTarget(response, testDestination, SUCCESS);

        getWorkerConnector(0).removeTest(1);
        response = sendMessage(testDestination);
        assertSingleTarget(response, testDestination.getParent(), FAILURE_TEST_NOT_FOUND);

        SimulatorAddress workerDestination = new SimulatorAddress(WORKER, 1, 1, 0);
        response = sendMessage(workerDestination);
        assertSingleTarget(response, workerDestination, SUCCESS);

        getAgentConnector(0).removeWorker(1);
        response = sendMessage(workerDestination);
        assertSingleTarget(response, workerDestination.getParent(), FAILURE_WORKER_NOT_FOUND);

        SimulatorAddress agentDestination = new SimulatorAddress(AGENT, 1, 0, 0);
        response = sendMessage(agentDestination);
        assertSingleTarget(response, agentDestination, SUCCESS);

        getCoordinatorConnector().removeAgent(1);
        response = sendMessage(agentDestination);
        assertSingleTarget(response, agentDestination.getParent(), FAILURE_AGENT_NOT_FOUND);
    }

    private static Response sendMessage(SimulatorAddress destination) throws Exception {
        SimulatorMessage message = buildMessage(destination);
        return sendFromCoordinator(message);
    }
}
