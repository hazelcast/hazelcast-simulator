package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import org.junit.Test;

import static com.hazelcast.simulator.testcontainer.TestPhase.LOCAL_TEARDOWN;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class CoordinatorParametersTest {

    @Test
    public void testConstructor() {
        SimulatorProperties properties = mock(SimulatorProperties.class);

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters(
                "CoordinatorParametersTest",
                properties,
                "workerClassPath",
                LOCAL_TEARDOWN,
                0,
                true,
                null);

        assertEquals("CoordinatorParametersTest", coordinatorParameters.getSessionId());
        assertEquals(properties, coordinatorParameters.getSimulatorProperties());
        assertEquals("workerClassPath", coordinatorParameters.getWorkerClassPath());
        assertEquals(LOCAL_TEARDOWN, coordinatorParameters.getLastTestPhaseToSync());
    }
}
