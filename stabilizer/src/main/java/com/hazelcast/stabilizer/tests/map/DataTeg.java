package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.waitClusterSize;
import static com.hazelcast.stabilizer.tests.utils.TestUtils.warmupPartitions;
import static org.junit.Assert.assertEquals;

public class DataTeg {

    private final static ILogger log = Logger.getLogger(DataTeg.class);

    public String basename = this.getClass().getName();
    public int maxItems = 10000;
    //TODO: This is super dangerous because we have no idea how many members there are going to be
    public int clusterSize = 6;
    public int nodeKillCount = 2;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Object, Object> map;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        waitClusterSize(log,targetInstance,clusterSize);
        warmupPartitions(log, targetInstance);

        for (int i = 0; i < maxItems; i++) {
            map.put(i, i);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        spawner.spawn(new Worker());
        spawner.awaitCompletion();
    }


    private class Worker implements Runnable {
        @Override
        public void run() {
            while (!testContext.isStopped()) {

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        log.info(basename + ": cluster size =" + targetInstance.getCluster().getMembers().size());
        log.info(basename + ": map size =" + map.size());

        if (targetInstance.getCluster().getMembers().size() == nodeKillCount) {
            log.info(basename + ": check again cluster =" + targetInstance.getCluster().getMembers().size());
        } else {
            log.info(basename + ": check again cluster =" + targetInstance.getCluster().getMembers().size());

            int max = 0;
            while (map.size() != maxItems) {

                Thread.sleep(1000);

                if (max++ == 60) {
                    break;
                }
            }

            assertEquals("data loss ", map.size(), maxItems);
            log.info(basename + "verify OK ");
        }
    }

    public static long nextKeyOwnedby(long key, HazelcastInstance instance) {
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
}
