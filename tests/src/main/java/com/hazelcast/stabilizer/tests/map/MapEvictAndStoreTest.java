package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.map.helpers.MapStoreWithCounterPerKey;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.stabilizer.tests.helpers.HazelcastTestUtils.isMemberNode;
import static junit.framework.Assert.assertEquals;

public class MapEvictAndStoreTest {

    private final static ILogger log = Logger.getLogger(MapEvictAndStoreTest.class);

    //properties
    public int threadCount = 10;
    public String basename = "MapEvictAndStore";
    public int timeToLiveSeconds = 10;
    public int writeDelaySeconds = 5;
    public int maxMapSize = 5000;
    public int logInterval = 10;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IAtomicLong keyCounter = null;
    private static final String MAPSTORECLASS = "com.hazelcast.stabilizer.tests.map.helpers.MapStoreWithCounterPerKey";
    
    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        keyCounter = targetInstance.getAtomicLong("MapEvictAndStoreTest.keyCounter");

        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
        mapConfig.setTimeToLiveSeconds(timeToLiveSeconds);
        mapConfig.getMaxSizeConfig()
                .setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.PER_NODE)
                .setSize(maxMapSize);
        
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true)
                .setClassName(MAPSTORECLASS)
                .setWriteDelaySeconds(writeDelaySeconds);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        //This setting fixes the duplicate inserts
        mapStoreConfig.setWriteCoalescing(true);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }


    public class Worker implements Runnable {
        @Override
        public void run() {
            IMap<Object, Object> map = targetInstance.getMap(basename);
            MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
            
            while (!testContext.isStopped()) {
                long keycounter = keyCounter.incrementAndGet();
                map.put(keycounter, "test value");

                if (keycounter % logInterval == 0) {
                    log.info("Iteration: " + keycounter + ". Map size: " + map.size() + " max per node is: " + maxMapSize);
                }
            }
        }
    }

    @Verify(global = false)
    public void globalVerify() throws Exception {
        if (isMemberNode(targetInstance)) {
            MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
            MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();

            Map<Object, AtomicInteger> mapStore = ((MapStoreWithCounterPerKey)mapStoreConfig.getImplementation()).storeCount;
            log.info(basename + ": Checking if some keys where stored more than once");

            for (Object k : mapStore.keySet()) {
                assertEquals("There were multiple calls to MapStore.store", 1, mapStore.get(k).intValue());
            }
        }
    }

}
