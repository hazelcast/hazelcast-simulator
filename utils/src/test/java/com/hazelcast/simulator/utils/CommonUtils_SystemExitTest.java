package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

public class CommonUtils_SystemExitTest {

    private static SecurityManager oldSecurityManager;

    private final Logger logger = Logger.getLogger(CommonUtils_SystemExitTest.class.getSimpleName());

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
    public void testExitWithError() {
        exitWithError();
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExitWithError_withCommandLineExitException() {
        exitWithError(logger, "expected failure", new CommandLineExitException("expected CommandLineExitException"));
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExitWithError_withThrowable() {
        exitWithError(logger, "expected failure", new Throwable("expected throwable"));
    }
}
