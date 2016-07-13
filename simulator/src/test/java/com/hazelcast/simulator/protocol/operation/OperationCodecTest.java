package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.WorkerParameters;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Collections;

import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.operation.IntegrationTestOperation.Type.EQUALS;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromJson;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.fromSimulatorMessage;
import static com.hazelcast.simulator.protocol.operation.OperationCodec.toJson;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class OperationCodecTest {

    private static final Logger LOGGER = Logger.getLogger(OperationCodecTest.class);

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(OperationCodec.class);
    }

    @Test
    public void testCodec_fromJson() {
        IntegrationTestOperation operation = new IntegrationTestOperation(EQUALS, "codecTest_fromJson");
        String json = toJson(operation);
        assertNotNull(json);

        IntegrationTestOperation decoded = (IntegrationTestOperation) fromJson(json, IntegrationTestOperation.class);
        assertEquals(operation.getTestData(), decoded.getTestData());
    }

    @Test
    public void testCodec_fromSimulatorMessage() {
        IntegrationTestOperation operation = new IntegrationTestOperation(EQUALS, "codecTest_fromSimulatorMessage");
        String json = toJson(operation);
        assertNotNull(json);

        SimulatorMessage message = new SimulatorMessage(COORDINATOR, COORDINATOR, 0, OperationType.INTEGRATION_TEST, json);
        IntegrationTestOperation decoded = (IntegrationTestOperation) fromSimulatorMessage(message);
        assertEquals(operation.getTestData(), decoded.getTestData());
    }

    @Test
    public void testCodec_withComplexOperation() {
        SimulatorProperties properties = mock(SimulatorProperties.class);

        WorkerParameters workerParameters = new WorkerParameters(
                properties,
                true,
                12345,
                "-verbose:gc -Xloggc:verbosegc.log",
                "",
                "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config" + NEW_LINE
                        + "  http://www.hazelcast.com/schema/config/hazelcast-config-3.6.xsd\"" + NEW_LINE
                        + "  xmlns=\"http://www.hazelcast.com/schema/config\"" + NEW_LINE
                        + "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" />",
                "",
                "",
                "",
                false
        );

        WorkerProcessSettings workerProcessSettings = new WorkerProcessSettings(1, WorkerType.MEMBER, workerParameters);

        CreateWorkerOperation operation = new CreateWorkerOperation(Collections.singletonList(workerProcessSettings),0);
        String json = toJson(operation);
        assertNotNull(json);
        LOGGER.info(json);

        CreateWorkerOperation decoded = (CreateWorkerOperation) fromJson(json, CreateWorkerOperation.class);
        WorkerProcessSettings decodedSettings = decoded.getWorkerJvmSettings().get(0);
        assertEquals(workerProcessSettings.getWorkerIndex(), decodedSettings.getWorkerIndex());
        assertEquals(workerProcessSettings.getWorkerType(), decodedSettings.getWorkerType());
        assertEquals(workerProcessSettings.getHazelcastConfig(), decodedSettings.getHazelcastConfig());
        assertEquals(workerProcessSettings.getJvmOptions(), decodedSettings.getJvmOptions());
        assertEquals(workerProcessSettings.isAutoCreateHzInstance(), decodedSettings.isAutoCreateHzInstance());
        assertEquals(workerProcessSettings.getWorkerStartupTimeout(), decodedSettings.getWorkerStartupTimeout());
        assertEquals(workerProcessSettings.getWorkerScript(), decodedSettings.getWorkerScript());
    }
}
