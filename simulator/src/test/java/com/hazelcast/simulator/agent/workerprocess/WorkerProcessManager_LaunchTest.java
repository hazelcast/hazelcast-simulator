package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.DummyPromise;
import com.hazelcast.simulator.agent.operations.CreateWorkerOperation;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.SimulatorProperties.DEFAULT_AGENT_PORT;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WorkerProcessManager_LaunchTest {
    private WorkerProcessManager workerProcessManager;
    private File workersHome;
    private Server server;

    @Before
    public void before() {
        File simulatorHome = setupFakeEnvironment();
        workersHome = new File(simulatorHome, "workers");

        server = mock(Server.class);
        workerProcessManager = new WorkerProcessManager(server,
                SimulatorAddress.fromString("C_A1"), "127.0.0.1", DEFAULT_AGENT_PORT);
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    @Test
    public void launch() throws Exception {
        workerProcessManager.setSessionId("mytest");

        String script = "echo foobar>>worker.address\n" +
                "sleep 1000\n";

        WorkerProcessSettings settings = new WorkerProcessSettings(
                1, WorkerType.MEMBER, "maven=3.8", script, 60, new HashMap<String, String>());
        CreateWorkerOperation op = new CreateWorkerOperation(singletonList(settings), 0);
        DummyPromise promise = new DummyPromise();

        workerProcessManager.launch(op, promise);

        promise.assertCompletesEventually();
        assertEquals("SUCCESS", promise.getAnswer());
        assertEquals(1, workerProcessManager.getWorkerProcesses().size());
    }

    @Test
    public void launch_whenFailed() throws Exception {
        workerProcessManager.setSessionId("mytest");

        String script = "exit 1";

        WorkerProcessSettings settings = new WorkerProcessSettings(
                1, WorkerType.MEMBER, "maven=3.8", script, 60, new HashMap<String, String>());
        CreateWorkerOperation op = new CreateWorkerOperation(singletonList(settings), 0);
        DummyPromise promise = new DummyPromise();

        workerProcessManager.launch(op, promise);

        promise.assertCompletesEventually();
        assertNotEquals("SUCCESS", promise.getAnswer());
        assertEquals(0, workerProcessManager.getWorkerProcesses().size());
        verify(server).sendCoordinator(any(FailureOperation.class));
    }

    // so what happens when a worker doesn't start up fast enough
    @Test
    public void launch_whenTimeout() throws Exception {
        workerProcessManager.setSessionId("mytest");

        String script = "sleep 10000";

        WorkerProcessSettings settings = new WorkerProcessSettings(
                1, WorkerType.MEMBER, "maven=3.8", script, 6, new HashMap<String, String>());
        CreateWorkerOperation op = new CreateWorkerOperation(singletonList(settings), 0);
        DummyPromise promise = new DummyPromise();

        workerProcessManager.launch(op, promise);

        promise.assertCompletesEventually();
        assertNotEquals("SUCCESS", promise.getAnswer());
        assertEquals(0, workerProcessManager.getWorkerProcesses().size());
        verify(server).sendCoordinator( any(FailureOperation.class));
    }
}
