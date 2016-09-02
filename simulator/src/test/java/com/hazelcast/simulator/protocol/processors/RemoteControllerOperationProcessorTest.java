package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static org.junit.Assert.assertEquals;

public class RemoteControllerOperationProcessorTest {

    private RemoteControllerOperationProcessor processor;

    @Before
    public void before() {
        processor = new RemoteControllerOperationProcessor();
    }

    @Test
    public void testProcessOperation_unsupportedOperation() {
        SimulatorOperation operation = new StopTestOperation();
        ResponseType responseType = processor.process(operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }
}
