package com.hazelcast.stabilizer.tests.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.map.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.query.helpers.Employee;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.Metronome;
import com.hazelcast.stabilizer.worker.SimpleMetronome;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class SqlPredicateBasicTest {

    private final static ILogger log = Logger.getLogger(SqlPredicateBasicTest.class);
    private final AtomicLong operations = new AtomicLong();

    //props
    public int threadCount = 10;
    public int keyLength = 10;
    public int keyCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public boolean usePut = true;
    public String basename = "SqlPredicateBasicTest";
    public KeyLocality keyLocality = KeyLocality.Random;

    //probes
    public IntervalProbe searchLatency;
    String sql = "age = 30 AND active = true";
    private int intervalMs;
    private IMap<String, Employee> map;
    private String[] keys;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(TestUtils.getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        keys = KeyUtils.generateKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());
        Random random = new Random();
        for (int k = 0; k < keyCount; k++) {
            String key = keys[k];
            int id = random.nextInt();
            map.put(key, new Employee(id));
        }
        log.info("Map size is:" + map.size());
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            long iteration = 0;
            Metronome metronome = SimpleMetronome.withFixedIntervalMs(intervalMs);
            final SqlPredicate sqlPredicate = new SqlPredicate(sql);
            while (!testContext.isStopped()) {
                metronome.waitForNext();
                searchLatency.started();
                map.values(sqlPredicate);
                searchLatency.done();
                
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }
        }

    }
}
