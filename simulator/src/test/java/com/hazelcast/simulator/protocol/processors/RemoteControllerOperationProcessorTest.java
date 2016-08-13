package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.RemoteControllerOperation.Type.LIST_COMPONENTS;
import static com.hazelcast.simulator.protocol.operation.RemoteControllerOperation.Type.RESPONSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class RemoteControllerOperationProcessorTest {

    private RemoteControllerOperationProcessor processor;

    @Before
    public void setUp() {
        processor = new RemoteControllerOperationProcessor();
    }

    @Test
    public void testProcessOperation_unsupportedOperation() {
        SimulatorOperation operation = new StopTestOperation();
        ResponseType responseType = processor.process(operation, COORDINATOR);

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }

    @Test
    public void testProcessRemoteControllerOperation() {
        RemoteControllerOperation operation = new RemoteControllerOperation(RESPONSE, "testPayload");

        ResponseType responseType = processor.process(operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.SUCCESS, responseType);
//        assertEquals(0, exceptionLogger.getExceptionCount());
        assertEquals("testPayload", processor.getResponse());
    }

    @Test
    public void testProcessRemoteControllerOperation_withUnsupportedType() {
        RemoteControllerOperation operation = new RemoteControllerOperation(LIST_COMPONENTS);

        ResponseType responseType = processor.process(operation, SimulatorAddress.COORDINATOR);

        assertEquals(ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    //    assertEquals(0, exceptionLogger.getExceptionCount());
        assertNull(processor.getResponse());
    }
}
