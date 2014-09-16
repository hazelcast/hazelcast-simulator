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
import com.hazelcast.spi.OperationService;
import com.hazelcast.stabilizer.TestCase;
import com.hazelcast.stabilizer.common.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.tests.BindException;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.String.format;

public class TestUtils {

    public static final String TEST_INSTANCE = "testInstance";

    public static byte[] randomByteArray(Random random, int length) {
        byte[] result = new byte[length];
        random.nextBytes(result);
        return result;
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
        private HazelcastInstance hz;

        @Override
        public Long call() throws Exception {
            Node node = getNode(hz);
            OperationService operationService = node.getNodeEngine().getOperationService();
            return operationService.getExecutedOperationCount();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }

    public static void warmupPartitions(ILogger logger, HazelcastInstance hz) {
        PartitionService partitionService = hz.getPartitionService();
        for (Partition partition : partitionService.getPartitions()) {
            while (partition.getOwner() == null) {
                logger.info("Partition owner is not yet set for partitionId: " + partition.getPartitionId());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
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
            return getField(hz, "original");
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl;
    }

    public static <E> E getField(Object o, String fieldName) {
        try {
            Field field = o.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (E) field.get(o);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void bindProperties(Object test, TestCase testCase) throws NoSuchFieldException, IllegalAccessException {
        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String property = entry.getKey();
            if ("class".equals(property)) {
                continue;
            }
            if (property.startsWith("probe-")) {
                continue;
            }
            String value = entry.getValue();
            bindProperty(test, property, value);
        }
    }

    public static ProbesConfiguration parseProbeConfiguration(TestCase testCase) {
        ProbesConfiguration configuration = new ProbesConfiguration();
        String probePrefix = "probe-";
        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String property = entry.getKey();
            if (property.startsWith(probePrefix)) {
                String probeName = property.substring(probePrefix.length());
                configuration.addConfig(probeName, entry.getValue());
            }
        }
        return configuration;
    }

    public static void bindProperty(Object test, String property, String value) throws IllegalAccessException {
        Field field = findPropertyField(test.getClass(), property);
        if (field == null) {
            throw new BindException(
                    format("Property [%s.%s] does not exist", test.getClass().getName(), property));
        }

        field.setAccessible(true);

        try {
            if (Boolean.TYPE.equals(field.getType())) {
                //primitive boolean
                if ("true".equals(value)) {
                    field.set(test, true);
                } else if ("false".equals(value)) {
                    field.set(test, false);
                } else {
                    throw new NumberFormatException("Unrecognized boolean value:" + value);
                }
            } else if (Boolean.class.equals(field.getType())) {
                //object boolean
                if ("null".equals(value)) {
                    field.set(test, null);
                } else if ("true".equals(value)) {
                    field.set(test, true);
                } else if ("false".equals(value)) {
                    field.set(test, false);
                } else {
                    throw new NumberFormatException("Unrecognized boolean value:" + value);
                }
            } else if (Byte.TYPE.equals(field.getType())) {
                //primitive byte
                field.set(test, Byte.parseByte(value));
            } else if (Byte.class.equals(field.getType())) {
                //object byte
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Byte.parseByte(value));
                }
            } else if (Short.TYPE.equals(field.getType())) {
                //primitive short
                field.set(test, Short.parseShort(value));
            } else if (Short.class.equals(field.getType())) {
                //object short
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Short.parseShort(value));
                }
            } else if (Integer.TYPE.equals(field.getType())) {
                //primitive integer
                field.set(test, Integer.parseInt(value));
            } else if (Integer.class.equals(field.getType())) {
                //object integer
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Integer.parseInt(value));
                }
            } else if (Long.TYPE.equals(field.getType())) {
                //primitive long
                field.set(test, Long.parseLong(value));
            } else if (Long.class.equals(field.getType())) {
                //object long
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Long.parseLong(value));
                }
            } else if (Float.TYPE.equals(field.getType())) {
                //primitive float
                field.set(test, Float.parseFloat(value));
            } else if (Float.class.equals(field.getType())) {
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Float.parseFloat(value));
                }
            } else if (Double.TYPE.equals(field.getType())) {
                //primitive double
                field.set(test, Double.parseDouble(value));
            } else if (Double.class.equals(field.getType())) {
                //object double
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Double.parseDouble(value));
                }
            } else if (String.class.equals(field.getType())) {
                //string
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, value);
                }
            } else if (field.getType().isEnum()) {
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    try {
                        Object enumValue = Enum.valueOf((Class<? extends Enum>) field.getType(), value);
                        field.set(test, enumValue);
                    } catch (IllegalArgumentException e) {
                        throw new NumberFormatException(e.getMessage());
                    }
                }
            } else {
                throw new BindException(
                        format("Unhandled type [%s] for field [%s.%s]", field.getType(), test.getClass().getName(), field.getName()));
            }
        } catch (NumberFormatException e) {
            throw new BindException(
                    format("Failed to bind value [%s] to property [%s.%s] of type [%s]",
                            value, test.getClass().getName(), property, field.getType())
            );
        }
    }

    public static Field findPropertyField(Class clazz, String property) {
        try {
            Field field = clazz.getDeclaredField(property);

            if (Modifier.isStatic(field.getModifiers())) {
                throw new BindException(
                        format("Property [%s.%s] can't be static", clazz.getName(), property));
            }

            if (Modifier.isFinal(field.getModifiers())) {
                throw new BindException(
                        format("Property [%s.%s] can't be final", clazz.getName(), property));
            }

            return field;
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                return null;
            } else {
                return findPropertyField(superClass, property);
            }
        }
    }

    private TestUtils() {
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


    public interface Targetable {
        public long getTarget();
    }

    public static void whileApproaching(Targetable targetable, long target) throws Exception {

        final long sleepMs=1500;
        final int maxStaticItterations=120;
        int staticCount=0;
        long delta, lastDelta=0;

        do{
            Thread.sleep(sleepMs);
            delta = Math.abs( targetable.getTarget() - target );

            if(Math.abs( lastDelta - delta ) == 0 ){
                staticCount++;
            }else{
                staticCount=0;
            }
            lastDelta=delta;

            System.out.println(" "+staticCount+" "+delta);

        } while ( delta != 0 && staticCount < maxStaticItterations ) ;
    }

    public static void whileApproachingZero(Targetable targetable) throws Exception{
        whileApproaching(targetable, 0);
    }
}
