package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

public class Ticket589 {

    private static final ILogger log = Logger.getLogger(Ticket589.class);

    private HazelcastInstance targetInstance;
    private TestContext testContext;
    public String basename = "MapStore1";
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
                IMap map = targetInstance.getMap(basename);
                log.info(basename + ": map size  =" + map.size());
            }
        }
    }

}





