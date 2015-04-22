package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThreadSpawner;

import java.util.Map;
import java.util.Random;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static junit.framework.TestCase.assertEquals;

public class MapEntryProcessorTest2 {

    public String basename = this.getClass().getName();
    public int threadCount = 10;
    public int keyCount = 1000;
    public int maxBizWorkIterations = 1000000;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Integer, Long> map;
    private IList<long[]> allIncrementsOnKeys;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        allIncrementsOnKeys = targetInstance.getList(basename + "Result");
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        allIncrementsOnKeys.destroy();
    }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int k = 0; k < keyCount; k++) {
            map.put(k, 0L);
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

    private class Worker implements Runnable {
        private final Random random = new Random();
        private final long[] localIncrementsAtKey = new long[keyCount];

        public void run() {
            while (!testContext.isStopped()) {
                int key = random.nextInt(keyCount);
                int increment = random.nextInt(100);
                int bizzWorkItterations = random.nextInt(maxBizWorkIterations);

                map.executeOnKey(key, new IncrementEntryProcessor(increment, bizzWorkItterations));
                localIncrementsAtKey[key] += increment;
            }
            //sleep to give time for the last EntryProcessor tasks to complete.
            sleepSeconds(5);
            allIncrementsOnKeys.add(localIncrementsAtKey);
        }
    }

    @Verify
    public void verify() throws Exception {
        long[] expectedValueForKey = new long[keyCount];

        for (long[] incrementsAtKey : allIncrementsOnKeys) {
            for (int k=0; k<incrementsAtKey.length; k++) {
                expectedValueForKey[k] += incrementsAtKey[k];
            }
        }

        for (int k = 0; k < keyCount; k++) {
            assertEquals( basename + ": expected value for key " + k, expectedValueForKey[k],  (long)map.get(k) );
        }
    }

    private static class IncrementEntryProcessor extends AbstractEntryProcessor<Integer, Long> {
        private final int increment;
        private final int maxBizWorkIterations;
        public volatile int bizWorkResult;

        private IncrementEntryProcessor(int increment, int maxBizWorkIterations) {
            this.increment = increment;
            this.maxBizWorkIterations = maxBizWorkIterations;
        }

        public Object process(Map.Entry<Integer, Long> entry) {
            long newValue = entry.getValue() + increment;
            entry.setValue(newValue);

            for (int work=0; work < maxBizWorkIterations; work++) {
                bizWorkResult += work % 13;
            }
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        MapEntryProcessorTest2 test = new MapEntryProcessorTest2();
        new TestRunner<MapEntryProcessorTest2>(test).run();
    }
}