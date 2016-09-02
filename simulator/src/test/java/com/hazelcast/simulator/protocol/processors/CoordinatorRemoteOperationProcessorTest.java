package com.hazelcast.simulator.protocol.processors;

import com.hazelcast.simulator.protocol.core.ResponseType;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.protocol.operation.StopTestOperation;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.ResponseType.UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.processors.OperationTestUtil.process;
import static org.junit.Assert.assertEquals;

public class CoordinatorRemoteOperationProcessorTest {

    private CoordinatorRemoteOperationProcessor processor;

    @Before
    public void before() {
        processor = new CoordinatorRemoteOperationProcessor();
    }

    @Test
    public void testProcessOperation_unsupportedOperation() throws Exception {
        SimulatorOperation operation = new StopTestOperation();
        ResponseType responseType = process(processor,operation,COORDINATOR);;

        assertEquals(UNSUPPORTED_OPERATION_ON_THIS_PROCESSOR, responseType);
    }
}
