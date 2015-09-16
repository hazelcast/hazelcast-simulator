package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.agent.workerjvm.WorkerJvmLauncher;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.exception.ExceptionLogger;
import com.hazelcast.simulator.protocol.operation.CreateWorkerOperation;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.operation.OperationType.getOperationType;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AgentOperationProcessor.class)
public class AgentOperationProcessorTest {

    private final ExceptionLogger exceptionLogger = mock(ExceptionLogger.class);

    private AgentOperationProcessor processor;

    @Before
    public void setUp() {
        processor = new AgentOperationProcessor(exceptionLogger, null, null);
    }

    @Test
    public void testProcessOperation_UnsupportedOperation() throws Exception {
        SimulatorOperation operation = new IntegrationTestOperation(IntegrationTestOperation.TEST_DATA);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void process_createWorker() throws Exception {
        WorkerJvmLauncher workerJvmLauncher = mock(WorkerJvmLauncher.class);
        whenNew(WorkerJvmLauncher.class).withAnyArguments().thenReturn(workerJvmLauncher);

        List<WorkerJvmSettings> workerJvmSettings = new ArrayList<WorkerJvmSettings>();
        workerJvmSettings.add(mock(WorkerJvmSettings.class));

        SimulatorOperation operation = new CreateWorkerOperation(workerJvmSettings);
        ResponseType responseType = processor.processOperation(getOperationType(operation), operation);

        assertEquals(SUCCESS, responseType);
    }
}
