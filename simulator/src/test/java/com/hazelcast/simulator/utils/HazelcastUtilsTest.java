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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.TestEnvironmentUtils.internalDistDirectory;
import static com.hazelcast.simulator.utils.ExecutorFactory.createScheduledThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.HazelcastUtils.createAddressConfig;
import static com.hazelcast.simulator.utils.HazelcastUtils.getHazelcastAddress;
import static com.hazelcast.simulator.utils.HazelcastUtils.initClientHzConfig;
import static com.hazelcast.simulator.utils.HazelcastUtils.initMemberHzConfig;
import static com.hazelcast.simulator.utils.HazelcastUtils.isMaster;
import static com.hazelcast.simulator.utils.HazelcastUtils.isOldestMember;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HazelcastUtilsTest {

    private static final int DELAY_SECONDS = 1;
    private static final InetSocketAddress SOCKET_ADDRESS = new InetSocketAddress("127.0.0.1", 5701);

    private static final ScheduledExecutorService executor = createScheduledThreadPool(1, HazelcastUtilsTest.class.getName());

    private HazelcastInstance hazelcastInstance;

    private Map<String, String> properties = new HashMap<String, String>();
    private ComponentRegistry componentRegistry;

    private String memberConfig;
    private String clientConfig;

    @Before
    public void before() {
        properties.put("VERSION_SPEC", "outofthebox");
        properties.put("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS", "1234");
        properties.put("JAVA_CMD", "java");

        componentRegistry = getComponentRegistryMock();

        memberConfig = fileAsText(new File(internalDistDirectory(), "conf/hazelcast.xml"));
        clientConfig = fileAsText(new File(internalDistDirectory(), "conf/client-hazelcast.xml"));
    }

    @AfterClass
    public static void afterClass() {
        executor.shutdown();
    }

    @Test
    public void testCreateAddressConfig() {
        String addressConfig = createAddressConfig("members", componentRegistry, "6666");
        for (int i = 1; i <= 5; i++) {
            assertTrue(addressConfig.contains("192.168.0." + i + ":6666"));
        }
    }

    @Test
    public void testInitMemberHzConfig() {
        properties.put("MANAGEMENT_CENTER_URL", "http://localhost:8080");
        properties.put("MANAGEMENT_CENTER_UPDATE_INTERVAL", "60");

        assertTrue(memberConfig.contains("<!--MEMBERS-->"));
        assertTrue(memberConfig.contains("<!--LICENSE-KEY-->"));
        assertTrue(memberConfig.contains("<!--MANAGEMENT_CENTER_CONFIG-->"));

        String memberHzConfig = initMemberHzConfig(memberConfig, componentRegistry, "licenseKey2342", properties, false);

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

        String clientHzConfig = initClientHzConfig(clientConfig, componentRegistry, properties, "licenseKey2342");

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

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(HazelcastUtils.class);
    }

    @Test
    public void testIsMaster_noHazelcastServerInstance() {
        assertFalse(isMaster(hazelcastInstance, executor, DELAY_SECONDS));
    }

    @Test
    public void testIsMaster_notOldestMember() {
        hazelcastInstance = createMockHazelcastInstance(false);
        assertFalse(isMaster(hazelcastInstance, executor, DELAY_SECONDS));
    }

    @Test
    public void testIsMaster_isOldestMember() {
        hazelcastInstance = createMockHazelcastInstance(true);
        assertTrue(isMaster(hazelcastInstance, executor, DELAY_SECONDS));
    }

    @Test(expected = IllegalStateException.class)
    public void testIsMaster_withExecutionException() {
        hazelcastInstance = createMockHazelcastInstance(true, new RuntimeException());
        isMaster(hazelcastInstance, executor, DELAY_SECONDS);
    }

    @Test(expected = IllegalStateException.class)
    public void testIsMaster_withInterruptedException() throws Exception {
        hazelcastInstance = createMockHazelcastInstance(true);
        ScheduledExecutorService executor = createMockScheduledExecutorService(new InterruptedException());

        isMaster(hazelcastInstance, executor, DELAY_SECONDS);
    }

    @Test(expected = IllegalStateException.class)
    public void testIsMaster_withTimeoutException() throws Exception {
        hazelcastInstance = createMockHazelcastInstance(true);
        ScheduledExecutorService executor = createMockScheduledExecutorService(new TimeoutException());

        isMaster(hazelcastInstance, executor, DELAY_SECONDS);
    }

    @Test
    public void testIsOldestMember_isOldest() {
        hazelcastInstance = createMockHazelcastInstance(true);
        assertTrue(isOldestMember(hazelcastInstance));
    }

    @Test
    public void testIsOldestMember_notOldest() {
        hazelcastInstance = createMockHazelcastInstance(false);
        assertFalse(isOldestMember(hazelcastInstance));
    }

    @Test
    public void testGetHazelcastAddress_withMemberWorker() {
        Member member = mock(Member.class);
        when(member.getSocketAddress()).thenReturn(SOCKET_ADDRESS);
        hazelcastInstance = createMockHazelcastInstance(member);

        String address = getHazelcastAddress(WorkerType.MEMBER, "172.16.16.1", hazelcastInstance);

        assertEquals("127.0.0.1:5701", address);
    }

    @Test
    public void testGetHazelcastAddress_withMemberWorker_hazelcastInstanceIsNull() {
        String address = getHazelcastAddress(WorkerType.MEMBER, "172.16.16.1", null);

        assertEquals("server:172.16.16.1", address);
    }

    @Test
    public void testGetHazelcastAddress_withMemberWorker_oldHazelcastVersion() {
        Member member = mock(Member.class);
        when(member.getInetSocketAddress()).thenReturn(SOCKET_ADDRESS);
        when(member.getSocketAddress()).thenThrow(new NoSuchMethodError("expected exception"));
        hazelcastInstance = createMockHazelcastInstance(member);

        String address = getHazelcastAddress(WorkerType.MEMBER, "172.16.16.1", hazelcastInstance);

        assertEquals("127.0.0.1:5701", address);
    }

    @Test
    public void testGetHazelcastAddress_withClientWorker() {
        Member member = mock(Member.class);
        when(member.getSocketAddress()).thenReturn(SOCKET_ADDRESS);
        hazelcastInstance = createMockHazelcastInstance(member);

        String address = getHazelcastAddress(WorkerType.JAVA_CLIENT, "172.16.16.1", hazelcastInstance);

        assertEquals("127.0.0.1:5701", address);
    }

    @Test
    public void testGetHazelcastAddress_withClientWorker_hazelcastInstanceIsNull() {
        String address = getHazelcastAddress(WorkerType.JAVA_CLIENT, "172.16.16.1", null);

        assertEquals("client:172.16.16.1", address);
    }

    @Test
    public void testGetHazelcastAddress_withClientWorker_oldHazelcastVersion() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getLocalMember()).thenThrow(new UnsupportedOperationException("Client has no local member!"));

        hazelcastInstance = mock(HazelcastInstance.class);
        when(hazelcastInstance.getLocalEndpoint()).thenThrow(new NoSuchMethodError("expected exception"));
        when(hazelcastInstance.getCluster()).thenReturn(cluster);

        String address = getHazelcastAddress(WorkerType.JAVA_CLIENT, "172.16.16.1", hazelcastInstance);

        assertEquals("client:172.16.16.1", address);
    }

    private HazelcastInstance createMockHazelcastInstance(boolean returnMember) {
        return createMockHazelcastInstance(returnMember, null);
    }

    private HazelcastInstance createMockHazelcastInstance(boolean returnMember, Exception getClusterException) {
        Member member = mock(Member.class);
        return createMockHazelcastInstance(member, returnMember, getClusterException);
    }

    private HazelcastInstance createMockHazelcastInstance(Member member) {
        return createMockHazelcastInstance(member, true, null);
    }

    private HazelcastInstance createMockHazelcastInstance(Member member, boolean returnMember, Exception getClusterException) {
        Set<Member> memberSet = new HashSet<Member>();
        memberSet.add(member);

        Cluster cluster = mock(Cluster.class);
        when(cluster.getMembers()).thenReturn(memberSet);
        when(cluster.getLocalMember()).thenReturn(returnMember ? member : null);

        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        when(hazelcastInstance.getLocalEndpoint()).thenReturn(returnMember ? member : null);
        if (getClusterException == null) {
            when(hazelcastInstance.getCluster()).thenReturn(cluster);
        } else {
            when(hazelcastInstance.getCluster()).thenReturn(cluster).thenThrow(getClusterException);
        }

        return hazelcastInstance;
    }

    private ScheduledExecutorService createMockScheduledExecutorService(Exception exceptionToThrow) throws Exception {
        ScheduledFuture future = mock(ScheduledFuture.class);
        when(future.get(anyInt(), eq(TimeUnit.SECONDS))).thenThrow(exceptionToThrow);
        when(future.get(anyLong(), eq(TimeUnit.SECONDS))).thenThrow(exceptionToThrow);

        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        when(executor.schedule(isA(Callable.class), anyInt(), eq(TimeUnit.SECONDS))).thenReturn(future);
        when(executor.schedule(isA(Callable.class), anyLong(), eq(TimeUnit.SECONDS))).thenReturn(future);

        return executor;
    }
}
