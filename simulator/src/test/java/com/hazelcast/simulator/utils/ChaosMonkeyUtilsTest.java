package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.TestEnvironmentUtils;
import com.hazelcast.simulator.protocol.operation.ChaosMonkeyOperation.Type;
import com.hazelcast.simulator.utils.helper.ExitStatusOneException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.simulator.utils.ChaosMonkeyUtils.DEFAULT_ALLOCATION_INTERVAL_MILLIS;
import static com.hazelcast.simulator.utils.ChaosMonkeyUtils.DEFAULT_SPIN_THREAD_COUNT;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class ChaosMonkeyUtilsTest {

    private static final int DEFAULT_TEST_TIMEOUT = 5000;

    @BeforeClass
    public static void setUp() throws Exception {
        TestEnvironmentUtils.setExitExceptionSecurityManager();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        TestEnvironmentUtils.resetSecurityManager();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ChaosMonkeyUtils.class);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testExecute_withIntegrationTest() throws Exception {
        ChaosMonkeyUtils.execute(Type.INTEGRATION_TEST);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testExecute_withBusySpin() {
        ChaosMonkeyUtils.setSpinThreadCount(1);

        ChaosMonkeyUtils.execute(Type.SPIN_CORE_INDEFINITELY);

        sleepMillis(500);

        ChaosMonkeyUtils.interruptThreads();
        ChaosMonkeyUtils.setSpinThreadCount(DEFAULT_SPIN_THREAD_COUNT);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void testExecute_withAllocateMemory() {
        ChaosMonkeyUtils.setAllocationIntervalMillis(200);

        ChaosMonkeyUtils.execute(Type.USE_ALL_MEMORY);

        sleepMillis(500);

        ChaosMonkeyUtils.interruptThreads();
        ChaosMonkeyUtils.setAllocationIntervalMillis(DEFAULT_ALLOCATION_INTERVAL_MILLIS);
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT, expected = ExitStatusOneException.class)
    public void testExecute_withSoftKill() {
        ChaosMonkeyUtils.execute(Type.SOFT_KILL);
    }
}
