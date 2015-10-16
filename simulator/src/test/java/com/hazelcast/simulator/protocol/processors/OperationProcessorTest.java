package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.OperationType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class OperationProcessorTest {

    @Test
    public void testShutdown_withInterruptedException() throws Exception {
        ExecutorService executorService = mock(ExecutorService.class);
        when(executorService.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("expected"));

        OperationProcessor processor = new OperationProcessor(null, executorService) {
            @Override
            protected ResponseType processOperation(OperationType operationType, SimulatorOperation operation) throws Exception {
                return null;
            }
        };

        processor.shutdown();

        verify(executorService).shutdown();
        verify(executorService).awaitTermination(anyLong(), any(TimeUnit.class));
        verifyNoMoreInteractions(executorService);
    }
}
