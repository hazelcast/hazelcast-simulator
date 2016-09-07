package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.utils.helper.ExitExceptionSecurityManager;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static com.hazelcast.simulator.utils.NativeUtils.getPidFromBeanString;
import static com.hazelcast.simulator.utils.NativeUtils.getPidFromManagementBean;
import static com.hazelcast.simulator.utils.NativeUtils.getPidViaReflection;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class NativeUtilsTest {

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
    public void testConstructor() throws Exception {
        invokePrivateConstructor(NativeUtils.class);
    }

    @Test
    public void testExecute() {
        NativeUtils.execute("pwd");
    }

    @Test(expected = ExitStatusOneException.class)
    public void testExecute_exitStatus() {
        NativeUtils.execute("pwd && false");
    }

    @Test(expected = ScriptException.class)
    public void testExecute_withException() {
        NativeUtils.execute("pwd && false", true);
    }

    @Test
    public void testGetPIDorNull() {
        // we should have at least one implementation which works on each build system
        Integer pid = getPID();
        assertNotNull(pid);
    }

    @Test
    public void testGetPidFromManagementBean() {
        // just to be sure it doesn't throw any unexpected errors
        getPidFromManagementBean();
    }

    @Test
    public void testGetPidViaReflection() {
        // just to be sure it doesn't throw any unexpected errors
        getPidViaReflection();
    }

    @Test
    public void testGetPidStringOrNull() {
        Integer pid = getPidFromBeanString("2342@localhost");
        assertNotNull(pid);
        assertEquals(2342, (int) pid);
    }

    @Test
    public void testGetPidStringOrNull_StringWithoutAdSign() {
        Integer pid = getPidFromBeanString("awkwardPidStringFromJvm");
        assertNull(pid);
    }

    @Test
    public void testGetPidStringOrNull_StringWithNonNumericPid() {
        Integer pid = getPidFromBeanString("test@localhost");
        assertNull(pid);
    }
}
