package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKeys;

public class MapTransactionReadWriteTest {

    enum Operation {
        PUT,
        GET
    }

    private static final ILogger LOGGER = Logger.getLogger(MapTransactionReadWriteTest.class);

    // properties
    public String basename = MapTransactionReadWriteTest.class.getSimpleName();
    public int threadCount = 10;
    public int keyLength = 10;
    public int valueLength = 10;
    public int keyCount = 10000;
    public int valueCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1;
    public KeyLocality keyLocality = KeyLocality.RANDOM;
    public int minNumberOfMembers = 0;

    public double putProb = 0.1;
    public boolean useSet = false;

    // probes
    public Probe putProbe;
    public Probe getProbe;

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
        LOGGER.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        waitClusterSize(LOGGER, targetInstance, minNumberOfMembers);
        keys = generateIntKeys(keyCount, Integer.MAX_VALUE, keyLocality, targetInstance);

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
                        putProbe.started();
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
                        putProbe.done();
                        break;
                    case GET:
                        getProbe.started();
                        targetInstance.executeTransaction(new TransactionalTask<Object>() {
                            @Override
                            public Object execute(TransactionalTaskContext transactionalTaskContext) throws TransactionException {
                                TransactionalMap<Integer, Integer> txMap = transactionalTaskContext.getMap(map.getName());
                                txMap.put(key, value);
                                return null;
                            }
                        });
                        getProbe.done();
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }

                iteration++;
                if (iteration % logFrequency == 0) {
                    LOGGER.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
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

    public static void main(String[] args) throws Exception {
        MapTransactionReadWriteTest test = new MapTransactionReadWriteTest();
        new TestRunner<MapTransactionReadWriteTest>(test).run();
    }
}
