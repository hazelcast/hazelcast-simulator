package com.hazelcast.stabilizer.tests.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.probes.probes.SimpleProbe;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.map.helpers.KeyUtils;
import com.hazelcast.stabilizer.tests.query.helpers.Employee;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.Metronome;
import com.hazelcast.stabilizer.worker.SimpleMetronome;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class SqlPredicateBasicTest {

    private final static ILogger log = Logger.getLogger(SqlPredicateBasicTest.class);
    private final AtomicLong operations = new AtomicLong();
    //props
    public int writePercentage = 10;
    public int threadCount = 10;
    public int keyLength = 10;
    public int keyCount = 10000;
    public int idCount = 10000;
    public int dataAmount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 10000;
    public boolean usePut = true;
    public String basename = "SqlPredicateBasicTest";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;

    String sql = "age between 30 and 60"; //it can be change from property file

    //probes
    public IntervalProbe searchLatency;
    public SimpleProbe throughput;
    private int intervalMs;
    private IMap<String, Employee> map;
    private String[] keys;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        if (writePercentage < 0) {
            throw new IllegalArgumentException("Write percentage can't be smaller than 0");
        }

        if (writePercentage > 100) {
            throw new IllegalArgumentException("Write percentage can't be larger than 100");
        }

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
        TestUtils.waitClusterSize(log, targetInstance, minNumberOfMembers);
        keys = KeyUtils.generateKeys(keyCount, keyLength, keyLocality, testContext.getTargetInstance());

        for (int k = 0; k < dataAmount; k++) { //need a configurable amount
            String key = keys[k];
            int id = KeyUtils.generateInt(idCount, keyLocality, testContext.getTargetInstance());
            map.put(key, new Employee(id));
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
                Collection<Employee> values = map.values(sqlPredicate);
                log.info("" + values);
                searchLatency.done();

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                iteration++;
                throughput.done();
            }
        }

    }
}
