package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
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

public class SplitClusterDataTest {

    private final static ILogger log = Logger.getLogger(SplitClusterDataTest.class);

    public String basename = this.getClass().getName();
    public int maxItems = 10000;
    public int clusterSize = -1;
    public int splitClusterSize = -1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Object, Object> map;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        if (clusterSize == -1 || splitClusterSize == -1) {
            throw new IllegalStateException("priorities: clusterSize == -1 Or splitClusterSize == -1");
        }
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        waitClusterSize(log,targetInstance, clusterSize);
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

                }
            }
        }
    }

    @Verify(global = false)
    public void verify() throws Exception {
        log.info(basename + ": cluster size =" + targetInstance.getCluster().getMembers().size());
        log.info(basename + ": map size =" + map.size());

        if (targetInstance.getCluster().getMembers().size() == splitClusterSize) {
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
}
