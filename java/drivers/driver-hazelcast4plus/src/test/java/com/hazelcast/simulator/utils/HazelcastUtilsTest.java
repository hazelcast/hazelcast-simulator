package com.hazelcast.simulator.utils;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.HazelcastUtils.getHazelcastAddress;
import static com.hazelcast.simulator.utils.HazelcastUtils.isMaster;
import static com.hazelcast.simulator.utils.HazelcastUtils.isOldestMember;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.hazelcast4plus.Hazelcast4PlusDriver.createAddressConfig;
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

    private static final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private HazelcastInstance hazelcastInstance;

    private Registry registry;

    @Before
    public void before() {
        registry = getComponentRegistryMock();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        executor.shutdown();
    }

    @Test
    public void testCreateAddressConfig() {
        String addressConfig = createAddressConfig("members", registry.getAgents(), "6666");
        for (int i = 1; i <= 5; i++) {
            assertTrue(addressConfig.contains("192.168.0." + i + ":6666"));
        }
    }

    private Registry getComponentRegistryMock() {
        List<AgentData> agents = new ArrayList<AgentData>();
        for (int i = 1; i <= 5; i++) {
            AgentData agent = mock(AgentData.class);
            when(agent.getPrivateAddress()).thenReturn("192.168.0." + i);
            agents.add(agent);
        }

        Registry registry = mock(Registry.class);
        when(registry.getAgents()).thenReturn(agents);
        return registry;
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

        String address = getHazelcastAddress("member", "172.16.16.1", hazelcastInstance);

        assertEquals("127.0.0.1:5701", address);
    }

    @Test
    public void testGetHazelcastAddress_withMemberWorker_hazelcastInstanceIsNull() {
        String address = getHazelcastAddress("member", "172.16.16.1", null);

        assertEquals("server:172.16.16.1", address);
    }

    @Test
    public void testGetHazelcastAddress_withClientWorker() {
        Member member = mock(Member.class);
        when(member.getSocketAddress()).thenReturn(SOCKET_ADDRESS);
        hazelcastInstance = createMockHazelcastInstance(member);

        String address = getHazelcastAddress("javaclient", "172.16.16.1", hazelcastInstance);

        assertEquals("127.0.0.1:5701", address);
    }

    @Test
    public void testGetHazelcastAddress_withClientWorker_hazelcastInstanceIsNull() {
        String address = getHazelcastAddress("javaclient", "172.16.16.1", null);

        assertEquals("client:172.16.16.1", address);
    }

    @Test
    public void testGetHazelcastAddress_withClientWorker_oldHazelcastVersion() {
        Cluster cluster = mock(Cluster.class);
        when(cluster.getLocalMember()).thenThrow(new UnsupportedOperationException("Client has no local member!"));

        hazelcastInstance = mock(HazelcastInstance.class);
        when(hazelcastInstance.getLocalEndpoint()).thenThrow(new NoSuchMethodError("expected exception"));
        when(hazelcastInstance.getCluster()).thenReturn(cluster);

        String address = getHazelcastAddress("javaclient", "172.16.16.1", hazelcastInstance);

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
