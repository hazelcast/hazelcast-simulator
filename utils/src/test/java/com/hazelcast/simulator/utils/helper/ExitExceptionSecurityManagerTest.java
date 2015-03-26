package com.hazelcast.simulator.utils.helper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExitExceptionSecurityManagerTest {

    private ExitExceptionSecurityManager exitExceptionSecurityManager = new ExitExceptionSecurityManager();

    @Test
    public void testCheckPermissionSingleArgument() throws Exception {
        exitExceptionSecurityManager.checkPermission(null);
    }

    @Test
    public void testCheckPermission() throws Exception {
        exitExceptionSecurityManager.checkPermission(null, null);
    }

    @Test
    public void testCheckExitStatusZero() throws Exception {
        exitExceptionSecurityManager.checkExit(0);
    }

    @Test(expected = ExitStatusOneException.class)
    public void testCheckExitStatusOne() throws Exception {
        exitExceptionSecurityManager.checkExit(1);
    }

    @Test
    public void testCheckExitStatusTwo() throws Exception {
        try {
            exitExceptionSecurityManager.checkExit(2);
        } catch (ExitException e) {
            assertEquals(2, e.getStatus());
            return;
        }
        fail("Expected ExitException!");
    }
}
