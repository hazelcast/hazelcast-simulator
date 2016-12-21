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
    public int gracePeriodMillis = 2000;

    private volatile LastClusterSizeChange lastClusterSizeChange;
    private IMap<Long, Long> map;
    private int quorumCount;

    @Setup
    @SuppressWarnings("unchecked")
    public void setup() {
        this.lastClusterSizeChange = new LastClusterSizeChange(0L,
                getMemberCount());
        this.map = targetInstance.getMap(name);
        this.quorumCount = targetInstance.getConfig()
                .getQuorumConfig("map-quorum-ref").getSize();
    }

    private int getMemberCount() {
        return targetInstance.getCluster().getMembers().size();
    }

    @TimeStep
    public void testPut(BaseThreadState state) {
        final long key = state.randomInt(keyCount);
        final int memberCount = getMemberCount();
        LastClusterSizeChange lastChange = lastClusterSizeChange;

        if (lastChange.timestamp + gracePeriodMillis > System
                .currentTimeMillis()) {
            return;
        }
        try {
            map.put(key, 0L);
            checkGracePeriod(lastChange, true);
        } catch (QuorumException qe) {
            checkGracePeriod(lastChange, false);
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
            logger.warning("Detected cluster change from "
                    + lastChange.clusterSize + " to " + memberCount);
            lastChange = this.lastClusterSizeChange = new LastClusterSizeChange(
                    System.currentTimeMillis(), memberCount);
        }
        if (lastChange.timestamp + gracePeriodMillis > System
                .currentTimeMillis()) {
            return;
        }
        Assert.fail(String
                .format("Quorum count was %s and the member count was %s but the operation %s.",
                        quorumCount, lastChange.clusterSize,
                        hadQuorum ? "failed" : "succeeded"));
    }

    private static class LastClusterSizeChange {
        final long timestamp;
        final int clusterSize;

        private LastClusterSizeChange(long timestamp, int clusterSize) {
            this.timestamp = timestamp;
            this.clusterSize = clusterSize;
        }
    }
}
