package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetSecurityManager;
import static com.hazelcast.simulator.TestEnvironmentUtils.setExitExceptionSecurityManager;

public class AgentTest {

    @BeforeClass
    public static void setUp() {
        setExitExceptionSecurityManager();
    }

    @AfterClass
    public static void tearDown() {
        resetSecurityManager();
    }

    @Test(expected = ExitStatusOneException.class)
    public void testMain_withException() {
        Agent.main(new String[]{});
    }
}
