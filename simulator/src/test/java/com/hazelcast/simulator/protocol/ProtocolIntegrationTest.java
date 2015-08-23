package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.CoordinatorConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.simulator.protocol.ProtocolUtil.assertAllTargets;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.buildMessage;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetLogLevel;
import static com.hazelcast.simulator.protocol.ProtocolUtil.resetMessageId;
import static com.hazelcast.simulator.protocol.ProtocolUtil.setLogLevel;
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
import static org.junit.Assert.assertEquals;

public class ProtocolIntegrationTest {

    private static final Logger LOGGER = Logger.getLogger(ProtocolIntegrationTest.class);

    private static CoordinatorConnector coordinatorConnector;
    private static List<AgentConnector> agentConnectors = Collections.synchronizedList(new ArrayList<AgentConnector>());
    private static List<WorkerConnector> workerConnectors = Collections.synchronizedList(new ArrayList<WorkerConnector>());

    @BeforeClass
    public static void setUp() {
        setLogLevel(Level.TRACE);

        workerConnectors.add(startWorker(1, 1, 10011));
        workerConnectors.add(startWorker(2, 1, 10012));

        workerConnectors.add(startWorker(1, 2, 10021));
        workerConnectors.add(startWorker(2, 2, 10022));

        agentConnectors.add(startAgent(1, 10001, "127.0.0.1", 10010));
        agentConnectors.add(startAgent(2, 10002, "127.0.0.1", 10020));

        coordinatorConnector = startCoordinator("127.0.0.1", 10000);

        resetMessageId();
    }

    @AfterClass
    public static void tearDown() {
        LOGGER.info("Shutdown of Coordinator...");
        if (coordinatorConnector != null) {
            coordinatorConnector.shutdown();
        }

        LOGGER.info("Shutdown of Agents...");
        for (AgentConnector agentConnector : agentConnectors) {
            agentConnector.shutdown();
        }

        LOGGER.info("Shutdown of Workers...");
        for (WorkerConnector workerConnector : workerConnectors) {
            workerConnector.shutdown();
        }

        LOGGER.info("Shutdown complete!");
        resetLogLevel();
    }

    @Test
    public void test_SingleAgent() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 1, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test
    public void test_AllAgents() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 0, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination, SUCCESS, 2);
    }

    @Test
    public void test_AgentNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 3, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_AGENT_NOT_FOUND);
    }

    @Test
    public void test_SingleWorker() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 1, 1, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test
    public void test_AllWorker() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 0, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination, SUCCESS, 4);
    }

    @Test
    public void test_WorkerNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 1, 3, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_WORKER_NOT_FOUND);
    }

    @Test
    public void test_AllAgents_WorkerNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 0, 3, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination.getParent(), FAILURE_WORKER_NOT_FOUND, 2);
    }

    @Test
    public void test_SingleTest() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 1, 1, 1);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test
    public void test_AllTests() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 0, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination, SUCCESS, 8);
    }

    @Test
    public void test_TestNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 1, 1, 3);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_TEST_NOT_FOUND);
    }

    @Test
    public void test_AllAgents_AllWorkers_TestNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 0, 0, 3);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination.getParent(), FAILURE_TEST_NOT_FOUND, 4);
    }

    @Ignore
    @Test(timeout = 500)
    public void test_toCoordinator_fromAgent() throws Exception {
        AgentConnector agent = agentConnectors.get(0);
        Response response = agent.write(buildMessage(SimulatorAddress.COORDINATOR, agent.getAddress()));

        assertSingleTarget(response, SimulatorAddress.COORDINATOR, SUCCESS);
    }

    @Ignore
    @Test(timeout = 500)
    public void test_toCoordinator_fromWorker() throws Exception {
        WorkerConnector worker = workerConnectors.get(0);
        Response response = worker.write(buildMessage(SimulatorAddress.COORDINATOR, worker.getAddress()));

        assertSingleTarget(response, SimulatorAddress.COORDINATOR, SUCCESS);
    }

    private static Response sendMessageAndAssertMessageId(SimulatorAddress destination) throws Exception {
        SimulatorMessage message = buildMessage(destination);
        Response response = coordinatorConnector.send(message);

        assertEquals(message.getMessageId(), response.getMessageId());

        return response;
    }
}
