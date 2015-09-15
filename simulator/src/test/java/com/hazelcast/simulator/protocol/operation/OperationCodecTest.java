package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OperationCodecTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(OperationCodec.class);
    }

    @Test
    public void testCodec_fromJson() {
        IntegrationTestOperation operation = new IntegrationTestOperation("codecTest_fromJson");
        String json = OperationCodec.toJson(operation);
        assertNotNull(json);

        IntegrationTestOperation decoded = (IntegrationTestOperation) OperationCodec.fromJson(json, IntegrationTestOperation.class);
        assertEquals(operation.getTestData(), decoded.getTestData());
    }

    @Test
    public void testCodec_fromSimulatorMessage() {
        IntegrationTestOperation operation = new IntegrationTestOperation("codecTest_fromSimulatorMessage");
        String json = OperationCodec.toJson(operation);
        assertNotNull(json);

        SimulatorMessage message = new SimulatorMessage(COORDINATOR, COORDINATOR, 0, OperationType.INTEGRATION_TEST, json);
        IntegrationTestOperation decoded = (IntegrationTestOperation) OperationCodec.fromSimulatorMessage(message);
        assertEquals(operation.getTestData(), decoded.getTestData());
    }
}
