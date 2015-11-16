package com.hazelcast.simulator.worker;

import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class ClientWorkerTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ClientWorker.class);
    }
}
