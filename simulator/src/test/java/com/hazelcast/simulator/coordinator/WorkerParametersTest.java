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

import static com.hazelcast.simulator.coordinator.WorkerParameters.createAddressConfig;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initClientHzConfig;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initMemberHzConfig;
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
    public void setUp() {
        properties = mock(SimulatorProperties.class);
        when(properties.getHazelcastVersionSpec()).thenReturn(HazelcastJARs.OUT_OF_THE_BOX);
        when(properties.get(eq("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS"))).thenReturn("1234");
        when(properties.get("PROFILER")).thenReturn(JavaProfiler.NONE.name());
        when(properties.get(eq("NUMA_CONTROL"), anyString())).thenReturn("none");

        componentRegistry = getComponentRegistryMock();

        memberConfig = fileAsText("dist/src/main/dist/conf/hazelcast.xml");
        clientConfig = fileAsText("dist/src/main/dist/conf/client-hazelcast.xml");
    }

    @Test
    public void testConstructor() {
        WorkerParameters workerParameters = new WorkerParameters(properties, true, 2342, "memberJvmOptions", "clientJvmOptions",
                memberConfig, clientConfig, "log4jConfig", false);

        assertTrue(workerParameters.isAutoCreateHzInstance());
        assertEquals(2342, workerParameters.getWorkerStartupTimeout());
        assertEquals(1234, workerParameters.getWorkerPerformanceMonitorIntervalSeconds());
        assertEquals(HazelcastJARs.OUT_OF_THE_BOX, workerParameters.getHazelcastVersionSpec());

        assertEquals("memberJvmOptions", workerParameters.getMemberJvmOptions());
        assertEquals("clientJvmOptions", workerParameters.getClientJvmOptions());

        assertEquals(memberConfig, workerParameters.getMemberHzConfig());
        assertEquals(clientConfig, workerParameters.getClientHzConfig());
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
    public void testGetRunPhaseLogIntervalSeconds_noPerformanceMonitor() {
        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, false);

        int intervalSeconds = workerParameters.getRunPhaseLogIntervalSeconds(5);
        assertEquals(5, intervalSeconds);
    }

    @Test
    public void testGetRunPhaseLogIntervalSeconds_withPerformanceMonitor_overPerformanceMonitorInterval() {
        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, true);

        int intervalSeconds = workerParameters.getRunPhaseLogIntervalSeconds(5000);
        assertEquals(1234, intervalSeconds);
    }

    @Test
    public void testGetRunPhaseLogIntervalSeconds_withPerformanceMonitor_belowPerformanceMonitorInterval() {
        WorkerParameters workerParameters = new WorkerParameters(properties, false, 0, null, null, null, null, null, true);

        int intervalSeconds = workerParameters.getRunPhaseLogIntervalSeconds(30);
        assertEquals(30, intervalSeconds);
    }

    @Test
    public void testCreateAddressConfig() {
        String addressConfig = createAddressConfig("members", componentRegistry, 6666);
        for (int i = 1; i <= 5; i++) {
            assertTrue(addressConfig.contains("192.168.0." + i + ":6666"));
        }
    }

    @Test
    public void testInitMemberHzConfig() {
        when(properties.get("MANAGEMENT_CENTER_URL")).thenReturn("http://localhost:8080");
        when(properties.get("MANAGEMENT_CENTER_UPDATE_INTERVAL")).thenReturn("60");

        assertTrue(memberConfig.contains("<!--MEMBERS-->"));
        assertTrue(memberConfig.contains("<!--LICENSE-KEY-->"));
        assertTrue(memberConfig.contains("<!--MANAGEMENT_CENTER_CONFIG-->"));

        String memberHzConfig = initMemberHzConfig(memberConfig, componentRegistry, 5701, "licenseKey2342", properties);

        assertNotNull(memberHzConfig);
        assertTrue(memberHzConfig.contains("licenseKey2342"));
        assertTrue(memberHzConfig.contains("http://localhost:8080"));

        assertFalse(memberHzConfig.contains("<!--MEMBERS-->"));
        assertFalse(memberHzConfig.contains("<!--LICENSE-KEY-->"));
        assertFalse(memberHzConfig.contains("<!--MANAGEMENT_CENTER_CONFIG-->"));
    }

    @Test
    public void testInitClientHzConfig() {
        assertTrue(clientConfig.contains("<!--MEMBERS-->"));
        assertTrue(clientConfig.contains("<!--LICENSE-KEY-->"));

        String clientHzConfig = initClientHzConfig(clientConfig, componentRegistry, 5701, "licenseKey2342");

        assertNotNull(clientHzConfig);
        assertTrue(clientHzConfig.contains("licenseKey2342"));

        assertFalse(clientHzConfig.contains("<!--MEMBERS-->"));
        assertFalse(clientHzConfig.contains("<!--LICENSE-KEY-->"));
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
