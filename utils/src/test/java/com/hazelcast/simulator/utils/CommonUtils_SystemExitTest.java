package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;

public class CommonUtils_SystemExitTest {

    private SecurityManager oldSecurityManager;
    private Logger logger;

    @Before
    public void setUp() {
        oldSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager());

        logger = Logger.getLogger(CommonUtils_SystemExitTest.class.getSimpleName());
    }

    @After
    public void tearDown() {
        System.setSecurityManager(oldSecurityManager);
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExitWithErrorWithThrowable() {
        exitWithError(logger, "expected failure", new Throwable("expected throwable"));
    }
}
