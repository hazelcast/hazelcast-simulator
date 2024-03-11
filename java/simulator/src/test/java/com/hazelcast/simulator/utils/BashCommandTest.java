package com.hazelcast.simulator.utils;


import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class BashCommandTest {

    @Test
    public void testExecute() {
        new BashCommand("pwd").execute();
    }

    @Test
    public void testExecute_exitStatus() {
        BashCommand cmd = new BashCommand("pwd && false");
        cmd.setThrowsExceptionOnError(true);
        assertThrows(ScriptException.class, ()-> cmd.execute());
    }

    @Test
    public void testExecute_withException() {
        BashCommand cmd = new BashCommand("pwd && false");
        cmd.setThrowsExceptionOnError(true);
        assertThrows(ScriptException.class, ()-> cmd.execute());
    }
}