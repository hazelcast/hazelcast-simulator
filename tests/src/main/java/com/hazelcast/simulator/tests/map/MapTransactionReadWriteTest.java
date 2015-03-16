package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.probes.probes.SimpleProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;

public class MapTransactionReadWriteTest {

    enum Operation {
        PUT,
        GET
    }

    private static final ILogger log = Logger.getLogger(MapTransactionReadWriteTest.class);

    // properties
    public int threadCount = 10;
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1;
    public String basename = "txintIntMap";
    public KeyLocality keyLocality = KeyLocality.Random;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;
    public boolean useSet = false;

    // probes
    public IntervalProbe putLatency;
    public IntervalProbe getLatency;
    public SimpleProbe throughput;

    private final AtomicLong operations = new AtomicLong();
    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Integer, Integer> map;
    private int[] keys;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename + "-" + testContext.getTestId());

        builder.addOperation(Operation.PUT, putProb).addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(log, targetInstance, minNumberOfMembers);
        keys = KeyUtils.generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, targetInstance);

        Random random = new Random();
        for (int key : keys) {
            int value = random.nextInt(Integer.MAX_VALUE);
            map.put(key, value);
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
        private final OperationSelector<Operation> selector = builder.build();
        private final Random random = new Random();

        @Override
        public void run() {
            long iteration = 0;
            while (!testContext.isStopped()) {

                final int key = randomKey();
                final int value = randomValue();

                switch (selector.select()) {
                    case PUT:
                        putLatency.started();
                        targetInstance.executeTransaction(new TransactionalTask<Object>() {
                            @Override
                            public Object execute(TransactionalTaskContext transactionalTaskContext) throws TransactionException {
                                TransactionalMap<Integer, Integer> txMap = transactionalTaskContext.getMap(map.getName());
                                if (useSet) {
                                    txMap.set(key, value);
                                } else {
                                    txMap.put(key, value);
                                }
                                return null;
                            }
                        });
                        putLatency.done();
                        break;
                    case GET:
                        getLatency.started();
                        targetInstance.executeTransaction(new TransactionalTask<Object>() {
                            @Override
                            public Object execute(TransactionalTaskContext transactionalTaskContext) throws TransactionException {
                                TransactionalMap<Integer, Integer> txMap = transactionalTaskContext.getMap(map.getName());
                                txMap.put(key, value);
                                return null;
                            }
                        }) ;
                        getLatency.done();
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

                throughput.done();
            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
        }

        private int randomKey() {
            int length = keys.length;
            return keys[random.nextInt(length)];
        }

        private int randomValue() {
            return random.nextInt(Integer.MAX_VALUE);
        }
    }

    public static void main(String[] args) throws Throwable {
        MapTransactionReadWriteTest test = new MapTransactionReadWriteTest();
        new TestRunner<MapTransactionReadWriteTest>(test).run();
    }
}
