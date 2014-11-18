package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.map.helpers.DelayMapLoader;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import static junit.framework.Assert.assertEquals;

public class Ticket589 {

    private static final ILogger log = Logger.getLogger(Ticket589.class);

    private HazelcastInstance targetInstance;
    private TestContext testContext;
    public String basename = this.getClass().getName();
    public int keyCount = 3500;
    public int keyLength = 10;
    private int threadCount = 1;

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            while (!testContext.isStopped()) {
                MapStoreConfig mapStoreConfig = targetInstance.getConfig().getMapConfig(basename).getMapStoreConfig();
                final DelayMapLoader mapLoader = (DelayMapLoader) mapStoreConfig.getImplementation();
                final IMap map = targetInstance.getMap(basename);

                log.info(basename + ": map size  =" + map.size());

                log.info(basename + ": " + mapLoader);

                for (Object k : map.localKeySet()) {
                    assertEquals(map.get(k), mapLoader.load(k));
                }
            }
        }
    }
}





