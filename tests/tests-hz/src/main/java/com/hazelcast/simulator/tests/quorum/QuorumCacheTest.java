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

package com.hazelcast.simulator.tests.quorum;

import com.hazelcast.quorum.QuorumException;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import org.junit.Assert;

import javax.cache.Cache;
import javax.cache.CacheManager;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * The QuorumCacheTest can be used to verify the Quorum behavior wherein
 * a continuous tests will assert the
 * 1. Adding member
 * 2. Removing member
 * and the tests will pass or fail accordingly.
 */

public class QuorumCacheTest extends HazelcastTest {

    // properties
    public int keyCount = 100;
    public int gracePeriodMillis = 2000;

    private volatile LastClusterSizeChange lastClusterSizeChange;
    private Cache<Long, Long> cache;
    private int quorumCount;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        CacheManager cacheManager = createCacheManager(targetInstance);
        cache = cacheManager.getCache(name);
        this.lastClusterSizeChange = new LastClusterSizeChange(0L,
                getMemberCount());
        this.quorumCount = targetInstance.getConfig()
                .getQuorumConfig("cache-quorum-ref").getSize();
    }

    private int getMemberCount() {
        return targetInstance.getCluster().getMembers().size();
    }

    @TimeStep
    public void testPut(BaseThreadState state) {
        final long key = state.randomInt(keyCount);
        LastClusterSizeChange lastChange = lastClusterSizeChange;

        if (lastChange.timestamp + gracePeriodMillis > System.currentTimeMillis()) {
            doProtectedOperation(key, 0L);
            return;
        }

        try {
            cache.put(key, 0L);
            checkGracePeriod(lastChange, true);
        } catch (QuorumException qe) {
            checkGracePeriod(lastChange, false);
        }
    }

    private void doProtectedOperation(Long key, long value) {
        try {
            cache.put(key, value);
            logger.warn("Detected Grace Period. Ignoring Operation succeeded behaviour.");
        } catch (QuorumException qe) {
            logger.warn("Detected Grace Period. Ignoring Quorum Exception.");
        }
    }

    private void checkGracePeriod(LastClusterSizeChange lastChange,
                                  boolean operationPassed) {
        boolean hadQuorum = lastChange.clusterSize >= quorumCount;
        if (operationPassed == hadQuorum) {
            return;
        }
        int memberCount = getMemberCount();
        if (lastChange.clusterSize != memberCount) {
            logger.warn("Detected cluster change from "
                    + lastChange.clusterSize + " to " + memberCount);
            lastChange = new LastClusterSizeChange(
                    System.currentTimeMillis(), memberCount);
            this.lastClusterSizeChange = lastChange;
        }
        if (lastChange.timestamp + gracePeriodMillis > System.currentTimeMillis()) {
            return;
        }
        Assert.fail(String
                .format("Quorum count was %s and the member count was %s but the operation %s.",
                        quorumCount, lastChange.clusterSize,
                        hadQuorum ? "failed" : "succeeded"));
    }

    private static final class LastClusterSizeChange {
        final long timestamp;
        final int clusterSize;

        private LastClusterSizeChange(long timestamp, int clusterSize) {
            this.timestamp = timestamp;
            this.clusterSize = clusterSize;
        }
    }
}
