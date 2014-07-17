package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Performance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Teardown;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class MapEntryProcessorTest {

    private final static ILogger log = Logger.getLogger(MapEntryProcessorTest.class);

    //props
    public String basename = this.getClass().getName();
    public int threadCount = 10;
    public int keyCount = 1000;
    public int minProcessorDelayMs = 0;
    public int maxProcessorDealyMs = 0;

    private IMap<Integer, Long> map;
    private IList<Map<Integer, Long>> resultsPerWorker;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
        resultsPerWorker = targetInstance.getList(basename+"ResultMap" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        resultsPerWorker.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int k = 0; k < keyCount; k++) {
            map.put(k, 0l);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Verify
    public void verify() throws Exception {
        long[] amount = new long[keyCount];

        for (Map<Integer, Long> map : resultsPerWorker) {
            for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                amount[entry.getKey()] += entry.getValue();
            }
        }

        int failures = 0;
        for (int k = 0; k < keyCount; k++) {
            long expected = amount[k];
            long found = map.get(k);
            if (expected != found) {
                failures++;
            }
        }

        assertEquals(0, failures);
    }

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final Map<Integer, Long> result = new HashMap<Integer, Long>();

        public void Worker(){
            for (int k = 0; k < keyCount; k++) {
                result.put(k, 0L);
            }
        }

        @Override
        public void run() {

            while (!testContext.isStopped()) {
                Integer key = random.nextInt(keyCount);
                long increment = random.nextInt(100);

                int delayMs=0;
                if(maxProcessorDealyMs!=0){
                    delayMs =  minProcessorDelayMs + random.nextInt(maxProcessorDealyMs);
                }

                map.executeOnKey(key, new IncrementEntryProcessor(increment, delayMs));
                increment(key, increment);
            }

            //sleep to give time for the last EntryProcessor tasks to complete.
            try {
                Thread.sleep(maxProcessorDealyMs * 2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            resultsPerWorker.add(result);
        }

        private void increment(int key, long increment) {
            result.put(key, result.get(key) + increment);
        }
    }

    private static class IncrementEntryProcessor extends AbstractEntryProcessor<Integer, Long> {
        private final long increment;
        private final long delayMs;

        private IncrementEntryProcessor(long increment, long delayMs) {
            this.increment = increment;
            this.delayMs = delayMs;
        }

        @Override
        public Object process(Map.Entry<Integer, Long> entry) {
            delay();
            entry.setValue(entry.getValue() + increment);
            return null;
        }

        private void delay(){
            if(delayMs!=0){
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Throwable {
        MapEntryProcessorTest test = new MapEntryProcessorTest();
        new TestRunner(test).run();
    }
}

