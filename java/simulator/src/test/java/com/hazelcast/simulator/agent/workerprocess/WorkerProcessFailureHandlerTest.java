package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.coordinator.messages.FailureMessage;
import com.hazelcast.simulator.protocol.Server;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class WorkerProcessFailureHandlerTest {

    private String agentAddress;
    private WorkerProcessFailureHandler handler;
    private Server server;

    @Before
    public void before() {
        agentAddress = "A1";
        server = mock(Server.class);
        handler = new WorkerProcessFailureHandler(agentAddress, server);
    }

    @Test
    public void testNonFatal() {
        WorkerProcess process = mock(WorkerProcess.class);
        handler.handle("foo", FailureType.WORKER_EXCEPTION, process, "1", "someerror");

        verify(server).sendCoordinator(any(FailureMessage.class));
    }

    @Test
    public void testIsPoison() {
        WorkerProcess process = mock(WorkerProcess.class);
        handler.handle("foo", FailureType.WORKER_NORMAL_EXIT, process, "1", "someerror");

        verify(server).sendCoordinator(any(FailureMessage.class));
    }
}
