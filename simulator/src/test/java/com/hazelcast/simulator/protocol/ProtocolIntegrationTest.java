package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.ProtocolUtil.assertAllTargets;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.buildMessage;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getAgentConnector;
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
import static org.junit.Assert.assertEquals;

public class ProtocolIntegrationTest {

    private static final int DEFAULT_TEST_TIMEOUT = 5000;

    @BeforeClass
    public static void setUp() {
        setLogLevel(Level.TRACE);

        startSimulatorComponents(2, 2, 2);
    }

    @AfterClass
    public static void tearDown() {
        stopSimulatorComponents();

        resetLogLevel();
        resetMessageId();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_SingleAgent() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 1, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_AllAgents() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 0, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination, SUCCESS, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_AgentNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 3, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_AGENT_NOT_FOUND);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_SingleWorker() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 1, 1, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_AllWorker() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 0, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination, SUCCESS, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_WorkerNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 1, 3, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_WORKER_NOT_FOUND);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_AllAgents_WorkerNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 0, 3, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination.getParent(), FAILURE_WORKER_NOT_FOUND, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_SingleTest() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 1, 1, 1);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_AllTests() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 0, 0, 0);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination, SUCCESS, 8);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_TestNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 1, 1, 3);
        Response response = sendMessageAndAssertMessageId(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_TEST_NOT_FOUND);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_AllAgents_AllWorkers_TestNotFound() throws Exception {
        SimulatorAddress destination = new SimulatorAddress(TEST, 0, 0, 3);
        Response response = sendMessageAndAssertMessageId(destination);

        assertAllTargets(response, destination.getParent(), FAILURE_TEST_NOT_FOUND, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_Coordinator_fromAgent() throws Exception {
        AgentConnector agent = getAgentConnector(0);
        Response response = agent.write(buildMessage(SimulatorAddress.COORDINATOR, agent.getAddress()));

        assertSingleTarget(response, SimulatorAddress.COORDINATOR, SUCCESS);
        assertEquals(agent.getAddress(), response.getDestination());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_Coordinator_fromWorker() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        Response response = worker.write(buildMessage(SimulatorAddress.COORDINATOR, worker.getAddress()));

        assertSingleTarget(response, SimulatorAddress.COORDINATOR, SUCCESS);
        assertEquals(worker.getAddress(), response.getDestination());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_Coordinator_fromTest() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress testAddress = worker.getAddress().getChild(1);
        Response response = worker.write(buildMessage(SimulatorAddress.COORDINATOR, testAddress));

        assertSingleTarget(response, SimulatorAddress.COORDINATOR, SUCCESS);
        assertEquals(testAddress, response.getDestination());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_SingleAgent_fromWorker() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress destination = getAgentConnector(0).getAddress();
        Response response = worker.write(buildMessage(destination, worker.getAddress()));

        assertSingleTarget(response, destination, SUCCESS);
        assertEquals(worker.getAddress(), response.getDestination());
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void test_SingleAgent_fromTest() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress testAddress = worker.getAddress().getChild(1);
        SimulatorAddress destination = getAgentConnector(0).getAddress();
        Response response = worker.write(buildMessage(destination, testAddress));

        assertSingleTarget(response, destination, SUCCESS);
        assertEquals(testAddress, response.getDestination());
    }

    private static Response sendMessageAndAssertMessageId(SimulatorAddress destination) throws Exception {
        SimulatorMessage message = buildMessage(destination);
        Response response = sendFromCoordinator(message);

        assertEquals(message.getMessageId(), response.getMessageId());

        return response;
    }
}
