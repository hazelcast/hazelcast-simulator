package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WorkerParametersTest {

    private SimulatorProperties properties;
    private ComponentRegistry componentRegistry;

    private String memberConfig;
    private String clientConfig;

    @Before
    public void setUp() throws Exception {
        properties = mock(SimulatorProperties.class);
        when(properties.getHazelcastVersionSpec()).thenReturn(HazelcastJARs.OUT_OF_THE_BOX);
        when(properties.get(eq("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS"))).thenReturn("1234");
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.NONE.name());
        when(properties.get(eq("NUMA_CONTROL"), anyString())).thenReturn("none");
        when(properties.get("MANAGEMENT_CENTER_URL")).thenReturn("http://localhost:8080");
        when(properties.get("MANAGEMENT_CENTER_UPDATE_INTERVAL")).thenReturn("60");

        componentRegistry = getComponentRegistryMock();

        memberConfig = fileAsText("dist/src/main/dist/conf/hazelcast.xml");
        clientConfig = fileAsText("dist/src/main/dist/conf/client-hazelcast.xml");
    }

    @Test
    public void testConstructor() {
        assertTrue(memberConfig.contains("<!--MEMBERS-->"));
        assertTrue(memberConfig.contains("<!--MANAGEMENT_CENTER_CONFIG-->"));

        assertTrue(clientConfig.contains("<!--MEMBERS-->"));

        WorkerParameters workerParameters = new WorkerParameters(properties, true, 2342, "memberJvmOptions", "clientJvmOptions",
                memberConfig, clientConfig, "log4jConfig", false, componentRegistry);

        assertTrue(workerParameters.isAutoCreateHzInstance());
        assertEquals(2342, workerParameters.getWorkerStartupTimeout());
        assertEquals(1234, workerParameters.getWorkerPerformanceMonitorIntervalSeconds());
        assertEquals(HazelcastJARs.OUT_OF_THE_BOX, workerParameters.getHazelcastVersionSpec());

        assertEquals("memberJvmOptions", workerParameters.getMemberJvmOptions());
        assertEquals("clientJvmOptions", workerParameters.getClientJvmOptions());

        assertNotNull(workerParameters.getMemberHzConfig());
        assertFalse(workerParameters.getMemberHzConfig().contains("<!--MEMBERS-->"));
        assertFalse(workerParameters.getMemberHzConfig().contains("<!--MANAGEMENT_CENTER_CONFIG-->"));

        assertNotNull(workerParameters.getClientHzConfig());
        assertFalse(workerParameters.getClientHzConfig().contains("<!--MEMBERS-->"));

        assertEquals("log4jConfig", workerParameters.getLog4jConfig());
        assertFalse(workerParameters.isMonitorPerformance());

        assertEquals(JavaProfiler.NONE, workerParameters.getProfiler());
        assertEquals("", workerParameters.getProfilerSettings());
        assertEquals("none", workerParameters.getNumaCtl());
    }

    @Test
    public void testConstructor_emptyProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn("");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false, null);

        assertEquals(JavaProfiler.NONE, workerParameters.getProfiler());
    }

    @Test
    public void testConstructor_withYourKitProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.YOURKIT.name());
        when(properties.get("YOURKIT_SETTINGS")).thenReturn("yourKitSettings");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false, null);

        assertEquals(JavaProfiler.YOURKIT, workerParameters.getProfiler());
        assertEquals("yourKitSettings", workerParameters.getProfilerSettings());
    }

    @Test
    public void testConstructor_withVtuneProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.VTUNE.name());
        when(properties.get("VTUNE_SETTINGS", "")).thenReturn("vtuneSettings");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false, null);

        assertEquals(JavaProfiler.VTUNE, workerParameters.getProfiler());
        assertEquals("vtuneSettings", workerParameters.getProfilerSettings());
    }

    @Test
    public void testGetRunPhaseLogIntervalSeconds_noPerformanceMonitor() {
        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false, null);

        int intervalSeconds = workerParameters.getRunPhaseLogIntervalSeconds(5);
        assertEquals(5, intervalSeconds);
    }

    @Test
    public void testGetRunPhaseLogIntervalSeconds_withPerformanceMonitor_overPerformanceMonitorInterval() {
        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, true, null);

        int intervalSeconds = workerParameters.getRunPhaseLogIntervalSeconds(5000);
        assertEquals(1234, intervalSeconds);
    }

    @Test
    public void testGetRunPhaseLogIntervalSeconds_withPerformanceMonitor_belowPerformanceMonitorInterval() {
        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, true, null);

        int intervalSeconds = workerParameters.getRunPhaseLogIntervalSeconds(30);
        assertEquals(30, intervalSeconds);
    }

    private ComponentRegistry getComponentRegistryMock() {
        List<AgentData> agents = new ArrayList<AgentData>();
        for (int i = 1; i <= 5; i++) {
            AgentData agentData = mock(AgentData.class);
            when(agentData.getPrivateAddress()).thenReturn("192.168.0." + i);
            agents.add(agentData);
        }

        ComponentRegistry componentRegistry = mock(ComponentRegistry.class);
        when(componentRegistry.getAgents()).thenReturn(agents);
        return componentRegistry;
    }
}
