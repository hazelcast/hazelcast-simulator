package com.hazelcast.simulator.agent;

import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AgentTest {

    private static SecurityManager oldSecurityManager;

    @BeforeClass
    public static void setUp() {
        oldSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager());
    }

    @AfterClass
    public static void tearDown() {
        System.setSecurityManager(oldSecurityManager);
    }

    @Test(expected = ExitStatusOneException.class)
    public void testMain_withException() {
        Agent.main(new String[]{});
    }
}
