package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.utils.AgentUtils.startAgents;
import static com.hazelcast.simulator.utils.AgentUtils.stopAgents;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AgentUtilsTest {

    private static final File AGENT_PID_FILE = new File("agent.pid");

    private static final Logger LOGGER = Logger.getLogger(AgentUtilsTest.class);

    private Bash bash;
    private SimulatorProperties simulatorProperties;
    private ComponentRegistry componentRegistry;

    @BeforeClass
    public static void setupEnvironment() {
        setDistributionUserDir();

        ensureExistingFile(AGENT_PID_FILE);
        writeText(String.valueOf(Integer.MAX_VALUE), AGENT_PID_FILE);
    }

    @AfterClass
    public static void resetEnvironment() {
        resetUserDir();

        deleteQuiet(AGENT_PID_FILE);
    }

    @Before
    public void setUp() {
        bash = mock(Bash.class);

        simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.getAgentPort()).thenReturn(9876);
        when(simulatorProperties.getAgentThreadPoolSize()).thenReturn(1);
        when(simulatorProperties.getWorkerLastSeenTimeoutSeconds()).thenReturn(3600);
        when(simulatorProperties.get("HARAKIRI_MONITOR_ENABLED")).thenReturn("true");

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("172.16.16.1", "127.0.0.1");
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(AgentUtils.class);
    }

    @Test
    public void testStartAgents_isStatic() {
        setCloudProvider(PROVIDER_STATIC);

        startAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        verify(bash).killAllJavaProcesses(eq("172.16.16.1"));
        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/agent "));
        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/.await-file-exists agent.pid"));
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testStartAgents_isEC2() {
        setCloudProvider(PROVIDER_EC2);

        startAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        verify(bash).killAllJavaProcesses(eq("172.16.16.1"));
        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/agent "));
        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/.await-file-exists agent.pid"));
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testStopAgents_isLocal() {
        setCloudProvider(PROVIDER_LOCAL);

        stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testStopAgents_isStatic() {
        setCloudProvider(PROVIDER_STATIC);

        stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/.kill-from-pid-file agent.pid"));
        verifyNoMoreInteractions(bash);
    }

    @Test
    public void testStopAgents_isEC2() {
        setCloudProvider(PROVIDER_EC2);

        stopAgents(LOGGER, bash, simulatorProperties, componentRegistry);

        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/.kill-from-pid-file agent.pid"));
        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/harakiri-monitor"));
        verifyNoMoreInteractions(bash);
    }

    private void setCloudProvider(String providerLocal) {
        when(simulatorProperties.getCloudProvider()).thenReturn(providerLocal);
    }
}
