package com.hazelcast.simulator.coordinator;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class AgentUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(AgentUtils.class);
    }
}
