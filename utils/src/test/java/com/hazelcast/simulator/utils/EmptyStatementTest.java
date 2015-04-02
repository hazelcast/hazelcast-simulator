package com.hazelcast.simulator.utils;

import org.junit.Test;

import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class EmptyStatementTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(EmptyStatement.class);
    }

    @Test
    public void testIgnore() throws Exception {
        ignore(new RuntimeException());
    }
}
