/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.tests.utils;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.OperationService;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

public class TestUtils {

    public static final String TEST_INSTANCE = "testInstance";
    private final static ILogger log = Logger.getLogger(TestUtils.class);

    /**
     * Assert that a certain task is going to assert to true eventually.
     *
     * This method makes use of an exponential back-off mechanism. So initially it will ask frequently, but the
     * more times it fails the less frequent the task is going to be retried.
     *
     * @param task
     * @param timeoutSeconds
     * @throws java.lang.NullPointerException if task is null.
     */
    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }

        AssertionError error;

        // the total timeout in ms.
        long timeoutMs = TimeUnit.SECONDS.toMillis(timeoutSeconds);

        // the time in ms when the assertTrue is going to expire.
        long expirationMs = System.currentTimeMillis() + timeoutMs;
        int sleepMillis = 100;

        for (; ; ) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }

            // there is a timeout, so we are done.
            if (System.currentTimeMillis() > expirationMs) {
                throw error;
            }

            sleepMillis(sleepMillis);
            sleepMillis *= 1.5;
        }
    }

    public static void sleepMillis(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Sleeps a random amount of time.
     *
     * @param random        the Random used to randomize
     * @param maxDelayNanos the maximum sleeping period in nano seconds. If maxDelayNanos equals or smaller than zero,
     *                      the call is ignored.
     */
    public static void sleepRandomNanos(Random random, long maxDelayNanos) {
        if (maxDelayNanos <= 0) {
            return;
        }

        long randomValue = Math.abs(random.nextLong());
        long delayNanos = randomValue % maxDelayNanos;
        LockSupport.parkNanos(delayNanos);
    }

    public static byte[] randomByteArray(Random random, int length) {
        byte[] result = new byte[length];
        random.nextBytes(result);
        return result;
    }

    public static String getPartitionDistributionInformation(HazelcastInstance hz) {
        Map<Member, Integer> partitionCountMap = new HashMap<Member, Integer>();
        int totalPartitions = 0;
        for(Partition partition: hz.getPartitionService().getPartitions()){
            totalPartitions++;
            Member member = partition.getOwner();
            Integer count = partitionCountMap.get(member);
            if(count == null){
                count = 0;
            }

            count++;
            partitionCountMap.put(member,count);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("total partitions:").append(totalPartitions).append("\n");
        for (Map.Entry<Member, Integer> entry : partitionCountMap.entrySet()) {
            Member member = entry.getKey();
            long count = entry.getValue();
            double percentage = count * 100d / totalPartitions;
            sb.append(member).append(" total=").append(count).append(" percentage=").append(percentage).append("%\n");
        }
        return sb.toString();
    }

    public static String getOperationCountInformation(HazelcastInstance hz) {
        Map<Member, Long> operationCountMap = getOperationCount(hz);

        long total = 0;
        for (Long count : operationCountMap.values()) {
            total += count;
        }

        StringBuffer sb = new StringBuffer();
        sb.append("total operations:").append(total).append("\n");
        for (Map.Entry<Member, Long> entry : operationCountMap.entrySet()) {
            Member member = entry.getKey();
            long count = entry.getValue();
            double percentage = count * 100d / total;
            sb.append(member).append(" total=").append(count).append(" percentage=").append(percentage).append("%\n");
        }
        return sb.toString();
    }

    public static Map<Member, Long> getOperationCount(HazelcastInstance hz) {
        IExecutorService executorService = hz.getExecutorService("operationCountExecutor");

        Map<Member, Future<Long>> futures = new HashMap<Member, Future<Long>>();
        for (Member member : hz.getCluster().getMembers()) {
            Future<Long> future = executorService.submitToMember(new GetOperationCount(), member);
            futures.put(member, future);
        }

        Map<Member, Long> result = new HashMap<Member, Long>();
        for (Map.Entry<Member, Future<Long>> entry : futures.entrySet()) {
            try {
                Member member = entry.getKey();
                Long value = entry.getValue().get();
                result.put(member, value);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        return result;
    }

    public final static class GetOperationCount implements Callable<Long>, HazelcastInstanceAware, Serializable {
        private transient HazelcastInstance hz;

        @Override
        public Long call() throws Exception {
            try {
                Node node = getNode(hz);
                OperationService operationService = node.getNodeEngine().getOperationService();
                return operationService.getExecutedOperationCount();
            } catch (NoSuchMethodError e) {
                log.warning(e);
                return -1l;
            }
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }

    public static void warmupPartitions(ILogger logger, HazelcastInstance hz) {
        logger.info("Waiting for partition warmup");

        PartitionService partitionService = hz.getPartitionService();
        long startTime = System.currentTimeMillis();
        for (Partition partition : partitionService.getPartitions()) {
            if (System.currentTimeMillis() - startTime > TimeUnit.MINUTES.toMillis(5)) {
                throw new IllegalStateException("Partition warmup timeout. Partitions didn't get an owner in time");
            }

            while (partition.getOwner() == null) {
                logger.finest("Partition owner is not yet set for partitionId: " + partition.getPartitionId());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        logger.info("Partitions are warmed up successfully");
    }

    public static void waitClusterSize(ILogger logger, HazelcastInstance hz, int clusterSize) throws InterruptedException {
        for (; ; ) {
            if (hz.getCluster().getMembers().size() >= clusterSize) {
                return;
            }

            logger.info("waiting cluster == " + clusterSize);
            Thread.sleep(1000);
        }
    }

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
        return impl != null ? impl.node : null;
    }

    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = null;
        if (hz instanceof HazelcastInstanceProxy) {
            return PropertyBindingSupport.getField(hz, "original");
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl;
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void sleepMs(long delayMs) {
        if (delayMs < 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
        }
    }

    public static long nextKeyOwnedBy(long key, HazelcastInstance instance) {
        final Member localMember = instance.getCluster().getLocalMember();
        final PartitionService partitionService = instance.getPartitionService();
        for (; ; ) {
            Partition partition = partitionService.getPartition(key);
            if (localMember.equals(partition.getOwner())) {
                return key;
            }
            key++;
        }
    }

    public static boolean isMemberNode(HazelcastInstance instance) {
        return instance instanceof HazelcastInstanceProxy;
    }

    public static boolean isClient(HazelcastInstance instance) {
        return !isMemberNode(instance);
    }

    // we don't want instances
    private TestUtils() {
    }
}
