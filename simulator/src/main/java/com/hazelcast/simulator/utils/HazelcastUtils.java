package com.hazelcast.simulator.utils;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.simulator.common.messaging.Message;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;

public final class HazelcastUtils {

    private static final int TIMEOUT_SECONDS = 60;

    private static final Logger LOGGER = Logger.getLogger(HazelcastUtils.class);

    private HazelcastUtils() {
    }

    public static boolean isMaster(final HazelcastInstance hazelcastInstance, ScheduledExecutorService executor,
                                   int delaySeconds) {
        if (hazelcastInstance == null || !isOldestMember(hazelcastInstance)) {
            return false;
        }
        try {
            return executor.schedule(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return isOldestMember(hazelcastInstance);
                }
            }, delaySeconds, TimeUnit.SECONDS).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        } catch (TimeoutException e) {
            throw new IllegalStateException(e);
        }
    }

    public static boolean isOldestMember(HazelcastInstance hazelcastInstance) {
        Iterator<Member> memberIterator = hazelcastInstance.getCluster().getMembers().iterator();
        return memberIterator.hasNext() && memberIterator.next().equals(hazelcastInstance.getLocalEndpoint());
    }

    public static void injectHazelcastInstance(HazelcastInstance hazelcastInstance, Message message) {
        if (message instanceof HazelcastInstanceAware) {
            if (hazelcastInstance != null) {
                ((HazelcastInstanceAware) message).setHazelcastInstance(hazelcastInstance);
            } else {
                LOGGER.warn(format("Message %s implements %s interface, but no instance is currently running in this worker.",
                        message.getClass().getName(), HazelcastInstanceAware.class));
            }
        }
    }
}
