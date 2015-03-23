package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * This tests the cas method: replace. So for optimistic concurrency control.
 *
 * We have a bunch of predefined keys, and we are going to concurrently increment the value and we protect ourselves against lost
 * updates using cas method replace.
 *
 * Locally we keep track of all increments, and if the sum of these local increments matches the global increment, we are done.
 */
public class MapCasTest {

    // properties
    public int threadCount = 10;
    public int keyCount = 1000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public String basename = "mapCas";

    private IMap<Integer, Long> map;
    private IMap<String, Map<Integer, Long>> resultsPerWorker;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
        resultsPerWorker = targetInstance.getMap("ResultMap" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @Verify
    public void verify() throws Exception {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> map : resultsPerWorker.values()) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
            }
        }

        int failures = 0;
        for (int i = 0; i < keyCount; i++) {
            long expected = amount[i];
            long found = map.get(i);
            if (expected != found) {
                failures++;
            }
        }

        assertEquals("There should not be any data races", 0, failures);
    }

    @RunWithWorker
    public AbstractWorker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        protected void beforeRun() {
            if (map.size() != keyCount) {
                throw new RuntimeException("Warmup has not run since the map is not filled correctly, found size: " + map.size());
            }

            for (int i = 0; i < keyCount; i++) {
                result.put(i, 0L);
            }
        }

        @Override
        protected void timeStep() {
            Integer key = randomInt(keyCount);
            long incrementValue = randomInt(100);

            for (;;) {
                Long current = map.get(key);
                Long update = current + incrementValue;
                if (map.replace(key, current, update)) {
                    increment(key, incrementValue);
                    break;
                }
            }
        }

        protected void afterRun() {
            resultsPerWorker.put(UUID.randomUUID().toString(), result);
        }

        private void increment(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    public static void main(String[] args) throws Throwable {
        MapCasTest test = new MapCasTest();
        new TestRunner<MapCasTest>(test).run();
    }
}
