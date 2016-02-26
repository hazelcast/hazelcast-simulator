package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.TargetType;
import org.junit.Test;

import static com.hazelcast.simulator.test.TestPhase.LOCAL_TEARDOWN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorParametersTest {

    @Test
    public void testConstructor() {
        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.getTargetType()).thenReturn(TargetType.PREFER_CLIENT);

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters(properties, "workerClassPath", false, true, false,
                true, false, LOCAL_TEARDOWN);

        assertEquals(properties, coordinatorParameters.getSimulatorProperties());
        assertEquals("workerClassPath", coordinatorParameters.getWorkerClassPath());
        assertFalse(coordinatorParameters.isUploadHazelcastJARs());
        assertTrue(coordinatorParameters.isEnterpriseEnabled());
        assertFalse(coordinatorParameters.isVerifyEnabled());
        assertTrue(coordinatorParameters.isParallel());
        assertFalse(coordinatorParameters.isRefreshJvm());
        assertEquals(TargetType.CLIENT, coordinatorParameters.getTargetType(true));
        assertEquals(TargetType.MEMBER, coordinatorParameters.getTargetType(false));
        assertEquals(0, coordinatorParameters.getTargetTypeCount());
        assertEquals(LOCAL_TEARDOWN, coordinatorParameters.getLastTestPhaseToSync());
    }
}
