package com.hazelcast.stabilizer.tests.map;


import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.util.Random;

public class Ticket589 {

    private static final ILogger log  = Logger.getLogger(Ticket589.class);

    private IMap map;
    private HazelcastInstance targetInstance;
    private TestContext testContext;
    public String basename = "map";

    private String[] keys;
    private String[] values;
    public KeyLocality keyLocality = KeyLocality.Random;
    public int keyCount = 3500;
    public int valueCount = 3500;
    public int keyLength = 10;
    public int valueLength = 10;
    private int threadCount = 1;

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Warmup
    public void warmup(){
        keys = KeyUtils.generateKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());
        values = StringUtils.generateStrings(valueCount, valueLength);
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

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                String key = randomKey();
                String value = randomValue();
                map.put(key, value);
            }
        }

        private String randomValue() {
            return values[random.nextInt(values.length)];
        }

        private String randomKey() {
            int length = keys.length;
            return keys[random.nextInt(length)];
        }
    }

    public static void main(String[] args) throws Throwable {
        Ticket589 test = new Ticket589();
        new TestRunner(test).run();
    }

}



