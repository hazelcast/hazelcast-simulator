package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.buildMessage;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startAgent;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startWorker;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;

public class ProtocolRemoveTargetTest {

    private static final Logger LOGGER = Logger.getLogger(ProtocolSmokeTest.class);

    private static CoordinatorConnector coordinatorConnector;
    private static AgentConnector agentConnector;
    private static WorkerConnector workerConnector;

    @Before
    public void setUp() {
        LOGGER.setLevel(Level.INFO);

        workerConnector = startWorker(1, 1, 10011, 1);

        agentConnector = startAgent(1, 10001, "127.0.0.1", 10010, 1);

        coordinatorConnector = startCoordinator("127.0.0.1", 10000, 1);
    }

    @After
    public void tearDown() {
        LOGGER.info("Shutdown of Coordinator...");
        if (coordinatorConnector != null) {
            coordinatorConnector.shutdown();
        }

        LOGGER.info("Shutdown of Agent...");
        if (agentConnector != null) {
            agentConnector.shutdown();
        }

        LOGGER.info("Shutdown of Worker...");
        if (workerConnector != null) {
            workerConnector.shutdown();
        }

        LOGGER.info("Shutdown complete!");
    }

    @Test
    public void test() throws Exception {
        SimulatorAddress testDestination = new SimulatorAddress(TEST, 1, 1, 1);

        Response response = sendMessage(testDestination);
        assertSingleTarget(response, testDestination, SUCCESS);

        workerConnector.removeTest(1);
        response = sendMessage(testDestination);
        assertSingleTarget(response, testDestination.getParent(), FAILURE_TEST_NOT_FOUND);

        SimulatorAddress workerDestination = new SimulatorAddress(WORKER, 1, 1, 0);
        response = sendMessage(workerDestination);
        assertSingleTarget(response, workerDestination, SUCCESS);

        agentConnector.removeWorker(1);
        response = sendMessage(workerDestination);
        assertSingleTarget(response, workerDestination.getParent(), FAILURE_WORKER_NOT_FOUND);

        SimulatorAddress agentDestination = new SimulatorAddress(AGENT, 1, 0, 0);
        response = sendMessage(agentDestination);
        assertSingleTarget(response, agentDestination, SUCCESS);

        coordinatorConnector.removeAgent(1);
        response = sendMessage(agentDestination);
        assertSingleTarget(response, agentDestination.getParent(), FAILURE_AGENT_NOT_FOUND);
    }

    private static Response sendMessage(SimulatorAddress destination) throws Exception {
        SimulatorMessage message = buildMessage(destination);
        return coordinatorConnector.send(message);
    }
}
