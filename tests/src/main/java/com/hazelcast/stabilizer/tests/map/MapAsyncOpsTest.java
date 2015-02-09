package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Performance;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.tests.map.helpers.MapOperationsCount;
import com.hazelcast.stabilizer.worker.selector.OperationSelector;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MapAsyncOpsTest {

    private enum Operation {
        PUT_ASYNC,
        PUT_ASYNC_TTL,
        GET_ASYNC,
        REMOVE_ASYNC,
        DESTROY
    }

    private final static ILogger log = Logger.getLogger(MapAsyncOpsTest.class);

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 10;
    public int maxTTLExpirySeconds = 3;

    public double putAsyncProb = 0.2;
    public double putAsyncTTLProb = 0.2;
    public double getAsyncProb = 0.2;
    public double removeAsyncProb = 0.2;
    public double destroyProb = 0.2;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private MapOperationsCount count = new MapOperationsCount();

    private OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    public MapAsyncOpsTest() {
    }

    @Performance
    public long getOperationCount() {
        return count.getTotalNoOfOps();
    }

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        operationSelectorBuilder.addOperation(Operation.PUT_ASYNC, putAsyncProb)
                                .addOperation(Operation.PUT_ASYNC_TTL, putAsyncTTLProb)
                                .addOperation(Operation.GET_ASYNC, getAsyncProb)
                                .addOperation(Operation.REMOVE_ASYNC, removeAsyncProb)
                                .addOperation(Operation.DESTROY, destroyProb);
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();

        IList<MapOperationsCount> results = targetInstance.getList(basename + "report");
        results.add(count);
    }

    private class Worker implements Runnable {
        private final OperationSelector<Operation> selector = operationSelectorBuilder.build();
        private final Random random = new Random();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    final int key = random.nextInt(keyCount);
                    final IMap<Integer, Object> map = targetInstance.getMap(basename);
                    switch (selector.select()) {
                        case PUT_ASYNC:
                            Object value = random.nextInt();
                            map.putAsync(key, value);
                            count.putAsyncCount.incrementAndGet();
                            break;
                        case PUT_ASYNC_TTL:
                            value = random.nextInt();
                            int delay = 1 + random.nextInt(maxTTLExpirySeconds);
                            map.putAsync(key, value, delay, TimeUnit.SECONDS);
                            count.putAsyncTTLCount.incrementAndGet();
                            break;
                        case GET_ASYNC:
                            map.getAsync(key);
                            count.getAsyncCount.incrementAndGet();
                            break;
                        case REMOVE_ASYNC:
                            map.removeAsync(key);
                            count.removeAsyncCount.incrementAndGet();
                            break;
                        case DESTROY:
                            map.destroy();
                            count.destroyCount.incrementAndGet();
                            break;
                    }
                } catch (DistributedObjectDestroyedException ignored) {
                }
            }
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        IList<MapOperationsCount> results = targetInstance.getList(basename + "report");
        MapOperationsCount total = new MapOperationsCount();
        for (MapOperationsCount i : results) {
            total.add(i);
        }
        log.info(basename + ": " + total + " total of " + results.size());
    }

    @Verify(global = false)
    public void verify() throws Exception {
        Thread.sleep(maxTTLExpirySeconds * 2);

        final IMap map = targetInstance.getMap(basename);
        log.info(basename + ": map size  =" + map.size());
    }

    public static void main(String[] args) throws Throwable {
        new TestRunner<MapAsyncOpsTest>(new MapAsyncOpsTest()).run();
    }
}