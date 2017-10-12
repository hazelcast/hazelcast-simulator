/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.AgentUtils.checkInstallation;
import static com.hazelcast.simulator.utils.AgentUtils.startAgents;
import static com.hazelcast.simulator.utils.AgentUtils.stopAgents;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.contains;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class AgentUtilsTest {

    private static final Logger LOGGER = Logger.getLogger(AgentUtilsTest.class);

    private Bash bash;
    private SimulatorProperties simulatorProperties;
    private ComponentRegistry componentRegistry;

    @Before
    public void before() {
        setupFakeEnvironment();
        writeText(String.valueOf(Integer.MAX_VALUE), new File(getUserDir(), "agent.pid"));

        bash = mock(Bash.class);

        simulatorProperties = mock(SimulatorProperties.class);
        when(simulatorProperties.getAgentPort()).thenReturn(9876);
        when(simulatorProperties.getAgentThreadPoolSize()).thenReturn(1);
        when(simulatorProperties.getWorkerLastSeenTimeoutSeconds()).thenReturn(3600);
        when(simulatorProperties.get("HARAKIRI_MONITOR_ENABLED")).thenReturn("true");

        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("172.16.16.1", "127.0.0.1");
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(AgentUtils.class);
    }

    @Test
    public void testCheckInstallation_isStatic_whenInstallationIsOkay() {
        setCloudProvider(PROVIDER_STATIC);

        when(bash.ssh(eq("172.16.16.1"), anyString(), anyBoolean(), anyBoolean())).thenReturn("Warning: foobar SIM-OK");

        checkInstallation(bash, simulatorProperties, componentRegistry);

        verify(bash).ssh(eq("172.16.16.1"), contains("/bin/agent"), anyBoolean(), anyBoolean());
        verifyNoMoreInteractions(bash);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCheckInstallation_isStatic_whenInstallationIsNotOkay() {
        setCloudProvider(PROVIDER_STATIC);

        when(bash.ssh(eq("172.16.16.1"), anyString(), anyBoolean(), anyBoolean())).thenReturn("Warning: foobar SIM-NOK");

        checkInstallation(bash, simulatorProperties, componentRegistry);
    }

    @Test(expected = CommandLineExitException.class)
    public void testCheckInstallation_isStatic_whenSshCommandFails() {
        setCloudProvider(PROVIDER_STATIC);

        when(bash.ssh(eq("172.16.16.1"), anyString(), anyBoolean(), anyBoolean())).thenThrow(
                new CommandLineExitException("expected exception", new CommandLineExitException("inner cause")));

        checkInstallation(bash, simulatorProperties, componentRegistry);
    }

    @Test
    public void testCheckInstallation_isLocal() {
        setCloudProvider(PROVIDER_LOCAL);

        checkInstallation(bash, simulatorProperties, componentRegistry);

        verifyZeroInteractions(bash);
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

        verify(bash).execute(contains("/bin/.kill-from-pid-file agent.pid"));
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
