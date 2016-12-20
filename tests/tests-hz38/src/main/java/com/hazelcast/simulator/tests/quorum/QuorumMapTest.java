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

import org.junit.Assert;

import com.hazelcast.core.IMap;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

public class QuorumMapTest extends AbstractTest {

    // properties
    public int keyCount = 100;
    public int gracePeriodInMilliSec = 2000;
    private volatile long lastInconsistencyTimestamp = 0L;
    private IMap<Long, Long> map;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        map = targetInstance.getMap(name);
    }

    @TimeStep
    public void testPut(BaseThreadState state) {
        final int quorumCount = targetInstance.getConfig()
                .getQuorumConfig("map-quorum-ref").getSize();
        boolean operationFailed = false;

        try {
            map.put((long) state.randomInt(keyCount), 0L);
            if (targetInstance.getCluster().getMembers().size() < quorumCount) {
                if (!(lastInconsistencyTimestamp > 0L)) {
                    startGracePeriodTimer();
                } else {
                    if (isGracePeriodElapsed()) {
                        operationFailed = true;
                    }
                }
            } else {
                resetGracePeriodTimer();
            }
        } catch (QuorumException qe) {
            if (targetInstance.getCluster().getMembers().size() >= quorumCount) {
                if (!(lastInconsistencyTimestamp > 0L)) {
                    startGracePeriodTimer();
                } else {
                    if (isGracePeriodElapsed()) {
                        operationFailed = true;
                    }
                }
            } else {
                resetGracePeriodTimer();
            }
        }

        if (operationFailed) {
            Assert.fail(String
                    .format("Quorum count was %s and the member count was %s but the operation %s.",
                            quorumCount,
                            targetInstance.getCluster().getMembers().size(),
                            hasQuorum(quorumCount, targetInstance.getCluster()
                                    .getMembers().size()) ? "failed"
                                    : "succeeded"));
        }
    }

    private boolean hasQuorum(int quorumCount, int memberCount) {
        return memberCount >= quorumCount;
    }

    private boolean isGracePeriodElapsed() {
        return (lastInconsistencyTimestamp + gracePeriodInMilliSec) < System
                .currentTimeMillis();
    }

    private void startGracePeriodTimer() {
        lastInconsistencyTimestamp = System.currentTimeMillis();
    }

    private void resetGracePeriodTimer() {
        lastInconsistencyTimestamp = 0L;
    }
}
