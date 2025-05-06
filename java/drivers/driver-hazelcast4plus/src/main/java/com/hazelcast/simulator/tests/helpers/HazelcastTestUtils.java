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
package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.OperationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static com.hazelcast.simulator.utils.ReflectionUtils.getFieldValue;
import static com.hazelcast.simulator.utils.VersionUtils.isMinVersion;
import static java.lang.String.format;
import static java.util.Comparator.comparingInt;
import static org.junit.Assert.fail;

public final class HazelcastTestUtils {

    private static final Logger LOGGER = LogManager.getLogger(HazelcastTestUtils.class);

    private HazelcastTestUtils() {
    }

    public static RuntimeException rethrow(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else {
            throw new TestException(throwable);
        }
    }

    public static void waitClusterSize(Logger logger, HazelcastInstance hz, int clusterSize) {
        for (; ; ) {
            if (hz.getCluster().getMembers().size() >= clusterSize) {
                return;
            }

            logger.info("waiting cluster == " + clusterSize);
            sleepSeconds(1);
        }
    }

    public static OperationService getOperationService(HazelcastInstance hz) {
        Node node = checkNotNull(getNode(hz), "node is null in Hazelcast instance " + hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        try {
            return nodeEngine.getOperationService();
        } catch (NoSuchMethodError e) {
            // fallback for a binary incompatible change (see commit http://git.io/vtfKU)
            return getOperationServiceViaReflection(nodeEngine);
        }
    }

    private static OperationService getOperationServiceViaReflection(NodeEngineImpl nodeEngine) {
        try {
            Method method = NodeEngineImpl.class.getMethod("getOperationService");
            return (OperationService) method.invoke(nodeEngine);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
        return impl != null ? impl.node : null;
    }

    private static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = null;
        if (hz instanceof HazelcastInstanceProxy) {
            return getFieldValue(hz, "original");
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl;
    }

    public static boolean isMemberNode(HazelcastInstance instance) {
        return instance instanceof HazelcastInstanceProxy;
    }

    public static boolean isClient(HazelcastInstance instance) {
        return !isMemberNode(instance);
    }

    public static void failOnVersionMismatch(String minVersion, String message) {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        String actualVersion = buildInfo.getVersion();
        LOGGER.info("Compare version " + actualVersion + " with minimum version " + minVersion + ": "
                + isMinVersion(minVersion, actualVersion));
        if (!isMinVersion(minVersion, actualVersion)) {
            fail(format(message, minVersion));
        }
    }

    /**
     * Assign an object to an index in a fair way so that the difference between min/max indexes assigned to any key is 1
     *
     * @param indexUpperBound The upper bound (exclusive) of the contiguous index range [0, indexUpperBound)
     * @param key The object to assign to an index
     * @param keysToIndexes The current mapping of keys to indexes
     * @return The index assigned
     * @param <K> The key type
     */
    public static <K> int assignKeyToIndex(int indexUpperBound, K key, Map<K, Integer> keysToIndexes) {
        if (keysToIndexes.containsKey(key)) return keysToIndexes.get(key);
        var indexChosenCounts = new int[indexUpperBound];
        keysToIndexes.values().forEach(i -> indexChosenCounts[i]++);
        int clientIndex = java.util.stream.IntStream.range(0, indexUpperBound).boxed().min(comparingInt(i -> indexChosenCounts[i])).orElse(0);
        keysToIndexes.put(key, clientIndex);
        return clientIndex;
    }
}
