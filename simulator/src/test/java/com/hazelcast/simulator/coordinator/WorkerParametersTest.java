package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.FileUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
                "memberHzConfig", "clientHzConfig", "log4jConfig", false);

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

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false);

        assertEquals(JavaProfiler.NONE, workerParameters.getProfiler());
    }

    @Test
    public void testConstructor_withYourKitProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.YOURKIT.name());
        when(properties.get("YOURKIT_SETTINGS")).thenReturn("yourKitSettings");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false);

        assertEquals(JavaProfiler.YOURKIT, workerParameters.getProfiler());
        assertEquals("yourKitSettings", workerParameters.getProfilerSettings());
    }

    @Test
    public void testConstructor_withVtuneProfiler() {
        properties = mock(SimulatorProperties.class);
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.VTUNE.name());
        when(properties.get("VTUNE_SETTINGS", "")).thenReturn("vtuneSettings");

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false);

        assertEquals(JavaProfiler.VTUNE, workerParameters.getProfiler());
        assertEquals("vtuneSettings", workerParameters.getProfilerSettings());
    }

    @Test
    public void testInitMemberHzConfig() {
        when(properties.get("MANAGEMENT_CENTER_URL")).thenReturn("http://localhost:8080");
        when(properties.get("MANAGEMENT_CENTER_UPDATE_INTERVAL")).thenReturn("60");

        String memberConfig = FileUtils.fileAsText("./dist/src/main/dist/conf/hazelcast.xml");
        ComponentRegistry componentRegistry = getComponentRegistryMock();

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, memberConfig, null, null,
                false);
        assertTrue(workerParameters.getMemberHzConfig().contains("<!--MEMBERS-->"));
        assertTrue(workerParameters.getMemberHzConfig().contains("<!--MANAGEMENT_CENTER_CONFIG-->"));

        workerParameters.initMemberHzConfig(componentRegistry, properties);
        assertFalse(workerParameters.getMemberHzConfig().contains("<!--MEMBERS-->"));
        assertFalse(workerParameters.getMemberHzConfig().contains("<!--MANAGEMENT_CENTER_CONFIG-->"));
    }

    @Test
    public void testInitClientHzConfig() {
        String memberConfig = FileUtils.fileAsText("./dist/src/main/dist/conf/hazelcast.xml");
        String clientConfig = FileUtils.fileAsText("./dist/src/main/dist/conf/client-hazelcast.xml");

        ComponentRegistry componentRegistry = getComponentRegistryMock();

        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, memberConfig, clientConfig,
                null, false);
        assertTrue(workerParameters.getClientHzConfig().contains("<!--MEMBERS-->"));

        workerParameters.initClientHzConfig(componentRegistry);
        assertFalse(workerParameters.getClientHzConfig().contains("<!--MEMBERS-->"));
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
