package com.hazelcast.simulator.tests.special;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.IWorker;
import com.hazelcast.simulator.worker.tasks.NoOperationWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.logPartitionStatistics;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class ClusterStatisticsTest {

    private static final ILogger LOGGER = Logger.getLogger(ClusterStatisticsTest.class);

    // properties
    public String basename = ClusterStatisticsTest.class.getSimpleName();
    public int isClusterSafeRetries = 10;

    HazelcastInstance hazelcastInstance;
    IMap<Object, Integer> map;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        this.hazelcastInstance = testContext.getTargetInstance();
        this.map = hazelcastInstance.getMap(basename);
    }

    @Warmup
    public void warmup() {
        PartitionService partitionService = hazelcastInstance.getPartitionService();

        int retry = 0;
        while (!partitionService.isClusterSafe() && retry++ < isClusterSafeRetries) {
            LOGGER.info(basename + ": isClusterSafe() " + partitionService.isClusterSafe());
            sleepSeconds(1);
        }
        LOGGER.info(basename + ": isClusterSafe() " + partitionService.isClusterSafe());
        LOGGER.info(basename + ": isLocalMemberSafe() " + partitionService.isLocalMemberSafe());
        LOGGER.info(basename + ": getCluster().getMembers().size() " + hazelcastInstance.getCluster().getMembers().size());

        logPartitionStatistics(LOGGER, basename, map, false);
    }

    @RunWithWorker
    public IWorker createWorker() {
        return new NoOperationWorker();
    }
}
