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
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static com.hazelcast.simulator.utils.ReflectionUtils.getFieldValue;
import static com.hazelcast.simulator.utils.VersionUtils.isMinVersion;
import static java.lang.String.format;
import static org.junit.Assert.fail;

public final class HazelcastTestUtils {

    private static final Logger LOGGER = Logger.getLogger(HazelcastTestUtils.class);

    private HazelcastTestUtils() {
    }

    public static RuntimeException rethrow(Throwable throwable) {
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        } else {
            throw new TestException(throwable);
        }
    }

    public static void logPartitionStatistics(Logger log, String name, IMap<Object, Integer> map, boolean printSizes) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapService mapService = (MapService) mapProxy.getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        Collection<Integer> localPartitions = mapServiceContext.getOwnedPartitions();
        int localSize = 0;
        StringBuilder partitionIDs = new StringBuilder();
        StringBuilder partitionSizes = new StringBuilder();
        String separator = "";
        for (int partitionId : localPartitions) {
            int partitionSize = mapServiceContext.getRecordStore(partitionId, map.getName()).size();
            localSize += partitionSize;
            partitionIDs.append(separator).append(partitionId);
            partitionSizes.append(separator).append(partitionSize);
            separator = ", ";
        }
        log.info(format("%s: Local partitions (count %d) (size %d) (avg %.2f) (IDs %s)%s",
                name, localPartitions.size(), localSize, localSize / (float) localPartitions.size(), partitionIDs.toString(),
                printSizes ? format(" (sizes %s)", partitionSizes.toString()) : ""));
    }

    public static void waitClusterSize(org.apache.log4j.Logger logger, HazelcastInstance hz, int clusterSize) {
        for (; ; ) {
            if (hz.getCluster().getMembers().size() >= clusterSize) {
                return;
            }

            logger.info("waiting cluster == " + clusterSize);
            sleepSeconds(1);
        }
    }

    public static InternalOperationService getOperationService(HazelcastInstance hz) {
        Node node = checkNotNull(getNode(hz), "node is null in Hazelcast instance " + hz);
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        try {
            return nodeEngine.getOperationService();
        } catch (NoSuchMethodError e) {
            // fallback for a binary incompatible change (see commit http://git.io/vtfKU)
            return getOperationServiceViaReflection(nodeEngine);
        }
    }

    private static InternalOperationService getOperationServiceViaReflection(NodeEngineImpl nodeEngine) {
        try {
            Method method = NodeEngineImpl.class.getMethod("getOperationService");
            return (InternalOperationService) method.invoke(nodeEngine);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(e);
        } catch (IllegalAccessException e) {
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

    /**
     * Returns the next {@code long} key owned by the given Hazelcast instance.
     *
     * @param instance Hazelcast instance to search next key for
     * @param lastKey  last key to start search from
     * @return next key owned by given Hazelcast instance
     */
    public static long nextKeyOwnedBy(HazelcastInstance instance, long lastKey) {
        Member localMember = instance.getCluster().getLocalMember();
        PartitionService partitionService = instance.getPartitionService();
        while (true) {
            Partition partition = partitionService.getPartition(lastKey);
            if (localMember.equals(partition.getOwner())) {
                return lastKey;
            }
            lastKey++;
        }
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
}
