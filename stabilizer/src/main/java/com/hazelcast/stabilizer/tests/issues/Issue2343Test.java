package com.hazelcast.stabilizer.tests.issues;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapService;
import com.hazelcast.stabilizer.tests.AbstractTest;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.util.scheduler.EntryTaskScheduler;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.stabilizer.Utils.sleepMillis;
import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.tests.TestUtils.getField;
import static com.hazelcast.stabilizer.tests.TestUtils.getNode;
import static com.hazelcast.stabilizer.tests.TestUtils.secondsToMillis;
import static java.lang.System.currentTimeMillis;

//https://github.com/hazelcast/hazelcast/issues/2343
public class Issue2343Test extends AbstractTest {

    private final static ILogger log = Logger.getLogger(Issue2343Test.class);

    private HazelcastInstance targetInstance;
    private IMap<Integer, Integer> map;
    private Node node;
    private EntryTaskScheduler idleEvictionScheduler;
    private ConcurrentMap idleScheduledEntries;

    //properties we can tinker with
    public int threadCount = 10;
    public int entryCountPerThread = 100;
    public int poundTimeSeconds = 60;
    public int expireTimeSeconds = 10;

    @Override
    public void localSetup() throws Exception {
        targetInstance = getTargetInstance();
        node = getNode(targetInstance);
        map = targetInstance.getMap("map");
        for (int k = 0; k < threadCount; k++) {
            spawn(new Worker());
        }

        MapService mapService = node.nodeEngine.getService(MapService.SERVICE_NAME);
        MapContainer container = mapService.getMapContainer(map.getName());
        idleEvictionScheduler = container.getIdleEvictionScheduler();

        idleScheduledEntries = getField(idleEvictionScheduler, "scheduledEntries");

        new MonitorThread().start();
    }

    private class MonitorThread extends Thread {
        public void run() {
            while (!stopped()) {
                sleepSeconds(10);
                log.info("idleScheduledEntries.size:" + idleScheduledEntries.size());
            }
        }
    }

    private class Worker implements Runnable {

        private Integer[] entries = new Integer[entryCountPerThread];
        private final Random random = new Random();

        @Override
        public void run() {
            while (!stopped()) {
                singleIteration();
            }
        }

        private void singleIteration() {
            init();
            pound();
            expire();
        }

        private void init() {
            //first we create random entries.
            for (int k = 0; k < entries.length; k++) {
                Integer key = random.nextInt();
                entries[k] = key;
                map.put(key, 0);
            }
        }

        private void pound() {
            long startTime = currentTimeMillis();
            while (startTime + secondsToMillis(poundTimeSeconds) > currentTimeMillis() && !stopped()) {
                Integer key = entries[random.nextInt(entries.length)];
                Integer value = map.get(key);
                if (value != null) {
                    map.put(key, value + 1);
                }

                sleepMillis(5);
            }
        }

        private void expire() {
            long startTime = currentTimeMillis();

            while (startTime + secondsToMillis(expireTimeSeconds) > currentTimeMillis() && !stopped()) {
                if (stopped()) {
                    return;
                }

                sleepMillis(500);
            }
        }
    }

    //just for local testing purposes.
    public static void main(String[] args) throws Exception {
        MapConfig mapConfig = new MapConfig("map");
        mapConfig.setMaxIdleSeconds(5);

        Config config = new Config();
        config.addMapConfig(mapConfig);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        Issue2343Test test = new Issue2343Test();

        TestRunner testRunner = new TestRunner();
        testRunner.setHazelcastInstance(instance);
        testRunner.run(test, 60000);
    }
}
