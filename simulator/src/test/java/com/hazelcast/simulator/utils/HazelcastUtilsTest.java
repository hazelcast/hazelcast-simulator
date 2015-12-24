package com.hazelcast.simulator.utils;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.common.messaging.MessageAddress;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.simulator.utils.ExecutorFactory.createScheduledThreadPool;
import static com.hazelcast.simulator.utils.HazelcastUtils.injectHazelcastInstance;
import static com.hazelcast.simulator.utils.HazelcastUtils.isMaster;
import static com.hazelcast.simulator.utils.HazelcastUtils.isOldestMember;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
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

    private final ScheduledExecutorService executor = createScheduledThreadPool(1, HazelcastUtilsTest.class);
    private final MessageAddress messageAddress = MessageAddress.builder().toAllAgents().toAllWorkers().toAllAgents().build();
    private final HazelcastAwareMessage injectMessage = new HazelcastAwareMessage(messageAddress);

    private HazelcastInstance hazelcastInstance;

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
    public void testInjectHazelcastInstance_noInstanceFound() {
        injectHazelcastInstance(null, injectMessage);
        assertFalse(injectMessage.isInstanceSet());
    }

    @Test
    public void testInjectHazelcastInstance_hazelcastServerInstance() {
        injectHazelcastInstance(createMockHazelcastInstance(false), injectMessage);
        assertTrue(injectMessage.isInstanceSet());
    }

    @Test
    public void testInjectHazelcastInstance_hazelcastClientInstance() {
        injectHazelcastInstance(createMockHazelcastInstance(false), injectMessage);
        assertTrue(injectMessage.isInstanceSet());
    }

    private HazelcastInstance createMockHazelcastInstance(boolean returnMember) {
        return createMockHazelcastInstance(returnMember, null);
    }

    private HazelcastInstance createMockHazelcastInstance(boolean returnMember, Exception getClusterException) {
        Member member = mock(Member.class);

        Set<Member> memberSet = new HashSet<Member>();
        memberSet.add(member);

        Cluster cluster = mock(Cluster.class);
        when(cluster.getMembers()).thenReturn(memberSet);

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

    private class HazelcastAwareMessage extends Message implements HazelcastInstanceAware {

        private boolean instanceSet;

        private HazelcastAwareMessage(MessageAddress messageAddress) {
            super(messageAddress);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            instanceSet = true;
        }

        private boolean isInstanceSet() {
            return instanceSet;
        }
    }
}
