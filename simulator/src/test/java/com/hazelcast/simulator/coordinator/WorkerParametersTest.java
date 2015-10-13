package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerParametersTest {

    private SimulatorProperties properties;

    @Before
    public void setUp() throws Exception {
        properties = mock(SimulatorProperties.class);
        when(properties.get(eq("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS"))).thenReturn("1234");
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.NONE.name());
        when(properties.get(eq("NUMA_CONTROL"), anyString())).thenReturn("none");
    }

    @Test
    public void testConstructor() {
        WorkerParameters workerParameters = new WorkerParameters(properties, true, 2342, "memberJvmOptions", "clientJvmOptions",
                "memberHzConfig", "clientHzConfig", "log4jConfig");

        assertEquals(2342, workerParameters.getWorkerStartupTimeout());
        assertEquals(1234, workerParameters.getWorkerPerformanceMonitorIntervalSeconds());
        assertTrue(workerParameters.isAutoCreateHzInstance());

        assertEquals("memberJvmOptions", workerParameters.getMemberJvmOptions());
        assertEquals("clientJvmOptions", workerParameters.getClientJvmOptions());

        assertEquals("memberHzConfig", workerParameters.getMemberHzConfig());
        assertEquals("clientHzConfig", workerParameters.getClientHzConfig());
        assertEquals("log4jConfig", workerParameters.getLog4jConfig());

        assertEquals(JavaProfiler.NONE, workerParameters.getProfiler());
        assertEquals("", workerParameters.getProfilerSettings());
        assertEquals("none", workerParameters.getNumaCtl());
    }

    @Test
    public void testConstructor_emptyProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn("");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null);

        assertEquals(JavaProfiler.NONE, workerParameters.getProfiler());
    }

    @Test
    public void testConstructor_withYourKitProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.YOURKIT.name());
        when(properties.get("YOURKIT_SETTINGS")).thenReturn("yourKitSettings");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null);

        assertEquals(JavaProfiler.YOURKIT, workerParameters.getProfiler());
        assertEquals("yourKitSettings", workerParameters.getProfilerSettings());
    }

    @Test
    public void testConstructor_withVtuneProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.VTUNE.name());
        when(properties.get("VTUNE_SETTINGS", "")).thenReturn("vtuneSettings");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null);

        assertEquals(JavaProfiler.VTUNE, workerParameters.getProfiler());
        assertEquals("vtuneSettings", workerParameters.getProfilerSettings());
    }

    @Test
    public void testSetter() {
        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null);

        workerParameters.setMemberHzConfig("newMemberHzConfig");
        assertEquals("newMemberHzConfig", workerParameters.getMemberHzConfig());

        workerParameters.setClientHzConfig("newClientHzConfig");
        assertEquals("newClientHzConfig", workerParameters.getClientHzConfig());
    }
}
