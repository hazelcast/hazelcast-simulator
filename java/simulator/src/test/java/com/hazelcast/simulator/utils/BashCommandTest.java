package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BashCommandTest {

    private SecurityManager oldSecurityManager;

    @Before
    public void before() {
        oldSecurityManager = System.getSecurityManager();
        System.setSecurityManager(new ExitExceptionSecurityManager(true));
    }

    @After
    public void after() {
        System.setSecurityManager(oldSecurityManager);
    }

    @Test
    public void testExecute() {
        new BashCommand("pwd").execute();
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExecute_exitStatus() {
        new BashCommand("pwd && false").execute();
    }

    @Test(expected = ScriptException.class)
    public void testExecute_withException() {
        new BashCommand("pwd && false").setThrowsExceptionOnError(true).execute();
    }
}