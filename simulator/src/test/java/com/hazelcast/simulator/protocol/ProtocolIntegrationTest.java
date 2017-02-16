package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.protocol.connector.AgentConnector;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.processors.TestOperationProcessor;
import com.hazelcast.simulator.utils.AssertTask;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_OPERATION;
import static com.hazelcast.simulator.protocol.ProtocolUtil.DEFAULT_TEST_TIMEOUT_MILLIS;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertAllTargets;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertEmptyFutureMaps;
import static com.hazelcast.simulator.protocol.ProtocolUtil.assertSingleTarget;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getAgentConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.getWorkerConnector;
import static com.hazelcast.simulator.protocol.ProtocolUtil.sendFromCoordinator;
import static com.hazelcast.simulator.protocol.ProtocolUtil.startSimulatorComponents;
import static com.hazelcast.simulator.protocol.ProtocolUtil.stopSimulatorComponents;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.TEST;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_AGENT_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_TEST_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.FAILURE_WORKER_NOT_FOUND;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.EQUALS;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertNotNull;

public class ProtocolIntegrationTest {

    private static final long ASSERT_EVENTUALLY_TIMEOUT_SECONDS = 3;

    @BeforeClass
    public static void beforeClass() {
        setupFakeUserDir();
        startSimulatorComponents(2, 2, 2);
    }

    @AfterClass
    public static void afterClass() {
        stopSimulatorComponents();
        teardownFakeUserDir();
    }

    @After
    public void commonAsserts() {
        assertEmptyFutureMaps();
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_SingleAgent() {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 1, 0, 0);
        Response response = sendFromCoordinator(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_AllAgents() {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 0, 0, 0);
        Response response = sendFromCoordinator(destination);

        assertAllTargets(response, destination, SUCCESS, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_AgentNotFound() {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 3, 0, 0);
        Response response = sendFromCoordinator(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_AGENT_NOT_FOUND);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_SingleWorker() {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 1, 1, 0);
        Response response = sendFromCoordinator(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_AllWorker() {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 0, 0, 0);
        Response response = sendFromCoordinator(destination);

        assertAllTargets(response, destination, SUCCESS, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_WorkerNotFound() {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 1, 3, 0);
        Response response = sendFromCoordinator(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_WORKER_NOT_FOUND);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_AllAgents_WorkerNotFound() {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 0, 3, 0);
        Response response = sendFromCoordinator(destination);

        assertAllTargets(response, destination.getParent(), FAILURE_WORKER_NOT_FOUND, 2);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_SingleTest() {
        SimulatorAddress destination = new SimulatorAddress(TEST, 1, 1, 1);
        Response response = sendFromCoordinator(destination);

        assertSingleTarget(response, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_AllTests() {
        SimulatorAddress destination = new SimulatorAddress(TEST, 0, 0, 0);
        Response response = sendFromCoordinator(destination);

        assertAllTargets(response, destination, SUCCESS, 8);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_TestNotFound() {
        SimulatorAddress destination = new SimulatorAddress(TEST, 1, 1, 3);
        Response response = sendFromCoordinator(destination);

        assertSingleTarget(response, destination.getParent(), FAILURE_TEST_NOT_FOUND);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_AllAgents_AllWorkers_TestNotFound() {
        SimulatorAddress destination = new SimulatorAddress(TEST, 0, 0, 3);
        Response response = sendFromCoordinator(destination);

        assertAllTargets(response, destination.getParent(), FAILURE_TEST_NOT_FOUND, 4);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_Coordinator_fromAgent() {
        AgentConnector agent = getAgentConnector(0);
        SimulatorAddress source = agent.getAddress();
        SimulatorAddress destination = COORDINATOR;
        Response response = agent.invoke(destination, DEFAULT_OPERATION);

        assertSingleTarget(response, source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_Coordinator_fromAgent_async() throws Exception {
        AgentConnector agent = getAgentConnector(0);
        SimulatorAddress source = agent.getAddress();
        SimulatorAddress destination = COORDINATOR;
        ResponseFuture future = agent.invokeAsync(destination, DEFAULT_OPERATION);

        assertSingleTarget(future.get(), source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_Coordinator_fromWorker() {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress();
        SimulatorAddress destination = COORDINATOR;
        Response response = worker.invoke(destination, DEFAULT_OPERATION);

        assertSingleTarget(response, source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_Coordinator_fromWorker_async() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress();
        SimulatorAddress destination = COORDINATOR;
        ResponseFuture future = worker.invokeAsync(destination, DEFAULT_OPERATION);

        assertSingleTarget(future.get(), source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_Coordinator_fromTest() {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress().getChild(1);
        SimulatorAddress destination = COORDINATOR;
        Response response = worker.invoke(source, destination, DEFAULT_OPERATION);

        assertSingleTarget(response, source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_Coordinator_fromTest_async() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress().getChild(1);
        SimulatorAddress destination = COORDINATOR;
        ResponseFuture future = worker.invokeAsync(source, destination, DEFAULT_OPERATION);

        assertSingleTarget(future.get(), source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_ParentAgent_fromWorker() {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress();
        SimulatorAddress destination = source.getParent();
        Response response = worker.invoke(destination, DEFAULT_OPERATION);

        assertSingleTarget(response, source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_ParentAgent_fromWorker_async() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress();
        SimulatorAddress destination = source.getParent();
        ResponseFuture future = worker.invokeAsync(destination, DEFAULT_OPERATION);

        assertSingleTarget(future.get(), source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_ParentAgent_fromTest() {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress().getChild(1);
        SimulatorAddress destination = worker.getAddress().getParent();
        Response response = worker.invoke(source, destination, DEFAULT_OPERATION);

        assertSingleTarget(response, source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_ParentAgent_fromTest_async() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress().getChild(1);
        SimulatorAddress destination = worker.getAddress().getParent();
        ResponseFuture future = worker.invokeAsync(source, destination, DEFAULT_OPERATION);

        assertSingleTarget(future.get(), source, destination, SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_SingleWorker_withExceptionDuringOperationProcessing() {
        SimulatorAddress destination = getWorkerConnector(0).getAddress();
        SimulatorOperation operation = new IntegrationTestOperation(EQUALS, "invalid");

        Response response = sendFromCoordinator(destination, operation);

        assertSingleTarget(response, destination, ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                //assertEquals(1, getCoordinatorConnector().getExceptionCount());
                assertEmptyFutureMaps();
            }
        }, ASSERT_EVENTUALLY_TIMEOUT_SECONDS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_LogOperation_SingleAgent() {
        SimulatorAddress destination = new SimulatorAddress(AGENT, 1, 0, 0);
        SimulatorOperation operation = new LogOperation("Please log me on " + destination + '!');

        Response response = sendFromCoordinator(destination, operation);

        assertSingleTarget(response, destination, ResponseType.SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_LogOperation_SingleWorker() {
        SimulatorAddress destination = new SimulatorAddress(WORKER, 1, 1, 0);
        SimulatorOperation operation = new LogOperation("Please log me on " + destination + '!', Level.WARN);

        Response response = sendFromCoordinator(destination, operation);

        assertSingleTarget(response, destination, ResponseType.SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_LogOperation_SingleTest() {
        SimulatorAddress destination = new SimulatorAddress(TEST, 1, 1, 1);
        SimulatorOperation operation = new LogOperation("Please log me on " + destination + '!', Level.ERROR);

        Response response = sendFromCoordinator(destination, operation);

        assertSingleTarget(response, destination, ResponseType.SUCCESS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT_MILLIS)
    public void test_LogOperation_Coordinator_fromTest() {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress().getChild(1);
        SimulatorAddress destination = COORDINATOR;

        SimulatorOperation operation = new LogOperation("Please log me on " + destination + '!', Level.FATAL);

        Response response = worker.invoke(source, destination, operation);

        assertSingleTarget(response, source, destination, ResponseType.SUCCESS);
    }

    @Test
    public void test_workerConnector_getTest() {
        WorkerConnector worker = getWorkerConnector(0);
        TestOperationProcessor processor = worker.getTest(1);

        assertNotNull(processor);
    }

    @Test
    public void test_WorkerConnector_submitFromTest() throws Exception {
        WorkerConnector worker = getWorkerConnector(0);
        SimulatorAddress source = worker.getAddress().getChild(1);
        SimulatorAddress destination = COORDINATOR;

        SimulatorOperation operation = new LogOperation("Please log me on " + destination + '!', Level.FATAL);

        ResponseFuture responseFuture = worker.submitFromTest(source, destination, operation);
        Response response = responseFuture.get();

        assertSingleTarget(response, source, destination, ResponseType.SUCCESS);
    }
}
