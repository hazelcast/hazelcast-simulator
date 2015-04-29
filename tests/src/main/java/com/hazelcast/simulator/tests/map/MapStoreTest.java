package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;
import com.hazelcast.simulator.tests.map.helpers.MapStoreWithCounter;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;

/**
 * This test operates on a map which has a {@link MapStore} configured.
 *
 * We use map operations such as put, get or delete with some probability distribution to trigger {@link MapStore} methods.
 * We verify that the the key/value pairs in the map are also "persisted" into the {@link MapStore}.
 */
public class MapStoreTest {

    private enum MapOperation {
        PUT,
        GET,
        GET_ASYNC,
        DELETE,
        DESTROY
    }

    private enum MapPutOperation {
        PUT,
        PUT_ASYNC,
        PUT_TTL,
        PUT_IF_ABSENT,
        REPLACE
    }

    private static final ILogger LOGGER = Logger.getLogger(MapStoreTest.class);

    public String basename = MapStoreTest.class.getSimpleName();
    public int keyCount = 10;

    public double putProb = 0.4;
    public double getProb = 0.2;
    public double getAsyncProb = 0.15;
    public double deleteProb = 0.2;
    public double destroyProb = 0.05;

    public double putUsingPutProb = 0.4;
    public double putUsingPutAsyncProb = 0.0;
    public double putUsingPutTTLProb = 0.3;
    public double putUsingPutIfAbsent = 0.15;
    public double putUsingReplaceProb = 0.15;

    public int mapStoreMaxDelayMs = 0;
    public int mapStoreMinDelayMs = 0;

    public int maxTTLExpiryMs = 3000;
    public int minTTLExpiryMs = 100;

    private final OperationSelectorBuilder<MapOperation> mapOperationOperationSelectorBuilder
            = new OperationSelectorBuilder<MapOperation>();
    private final OperationSelectorBuilder<MapPutOperation> mapPutOperationOperationSelectorBuilder
            = new OperationSelectorBuilder<MapPutOperation>();

    private int putTTlKeyDomain;
    private int putTTlKeyRange;

    private HazelcastInstance targetInstance;
    private IMap<Integer, Integer> map;
    private IList<MapOperationCounter> operationCounterList;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        targetInstance = testContext.getTargetInstance();
        putTTlKeyDomain = keyCount;
        putTTlKeyRange = keyCount;

        map = targetInstance.getMap(basename);
        operationCounterList = targetInstance.getList(basename + "report");

        MapStoreWithCounter.setMinMaxDelayMs(mapStoreMinDelayMs, mapStoreMaxDelayMs);

        mapOperationOperationSelectorBuilder.addOperation(MapOperation.PUT, putProb)
                .addOperation(MapOperation.GET, getProb)
                .addOperation(MapOperation.GET_ASYNC, getAsyncProb)
                .addOperation(MapOperation.DELETE, deleteProb)
                .addOperation(MapOperation.DESTROY, destroyProb);

        mapPutOperationOperationSelectorBuilder.addOperation(MapPutOperation.PUT, putUsingPutProb)
                .addOperation(MapPutOperation.PUT_ASYNC, putUsingPutAsyncProb)
                .addOperation(MapPutOperation.PUT_TTL, putUsingPutTTLProb)
                .addOperation(MapPutOperation.PUT_IF_ABSENT, putUsingPutIfAbsent)
                .addOperation(MapPutOperation.REPLACE, putUsingReplaceProb);
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        MapOperationCounter total = new MapOperationCounter();
        for (MapOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        LOGGER.info(basename + ": " + total + " from " + operationCounterList.size() + " worker threads");
    }

    @Verify(global = false)
    public void verify() throws Exception {
        if (isClient(targetInstance)) {
            return;
        }

        MapStoreConfig mapStoreConfig = targetInstance.getConfig().getMapConfig(basename).getMapStoreConfig();
        int writeDelaySeconds = mapStoreConfig.getWriteDelaySeconds();

        sleepMillis(mapStoreMaxDelayMs * 2 + maxTTLExpiryMs * 2 + ((writeDelaySeconds * 2) * 1000));

        final MapStoreWithCounter mapStore = (MapStoreWithCounter) mapStoreConfig.getImplementation();

        LOGGER.info(basename + ": map size = " + map.size());
        LOGGER.info(basename + ": map store = " + mapStore);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (Integer key : map.localKeySet()) {
                    assertEquals(map.get(key), mapStore.get(key));
                }
                assertEquals("Map entrySets should be equal", map.getAll(map.localKeySet()).entrySet(), mapStore.entrySet());

                for (int key = putTTlKeyDomain; key < putTTlKeyDomain + putTTlKeyRange; key++) {
                    assertNull(basename + ": TTL key should not be in the map", map.get(key));
                }
            }
        });
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<MapOperation> {

        private final MapOperationCounter operationCounter = new MapOperationCounter();
        private final OperationSelector<MapPutOperation> mapPutOperationSelector
                = mapPutOperationOperationSelectorBuilder.build();

        public Worker() {
            super(mapOperationOperationSelectorBuilder);
        }

        @Override
        public void timeStep(MapOperation mapOperation) {
            Integer key = randomInt(keyCount);

            switch (mapOperation) {
                case PUT:
                    putOperation(key);
                    break;
                case GET:
                    map.get(key);
                    operationCounter.getCount.incrementAndGet();
                    break;
                case GET_ASYNC:
                    map.getAsync(key);
                    operationCounter.getAsyncCount.incrementAndGet();
                    break;
                case DELETE:
                    map.delete(key);
                    operationCounter.deleteCount.incrementAndGet();
                    break;
                case DESTROY:
                    map.destroy();
                    operationCounter.destroyCount.incrementAndGet();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private void putOperation(Integer key) {
            Integer value = randomInt();

            switch (mapPutOperationSelector.select()) {
                case PUT:
                    map.put(key, value);
                    operationCounter.putCount.incrementAndGet();
                    break;
                case PUT_ASYNC:
                    map.putAsync(key, value);
                    operationCounter.putAsyncCount.incrementAndGet();
                    break;
                case PUT_TTL:
                    int delayKey = putTTlKeyDomain + randomInt(putTTlKeyRange);
                    int delayMs = minTTLExpiryMs + randomInt(maxTTLExpiryMs);
                    map.putTransient(delayKey, value, delayMs, TimeUnit.MILLISECONDS);
                    operationCounter.putTransientCount.incrementAndGet();
                    break;
                case PUT_IF_ABSENT:
                    map.putIfAbsent(key, value);
                    operationCounter.putIfAbsentCount.incrementAndGet();
                    break;
                case REPLACE:
                    Integer orig = map.get(key);
                    if (orig != null) {
                        map.replace(key, orig, value);
                        operationCounter.replaceCount.incrementAndGet();
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected void afterRun() {
            operationCounterList.add(operationCounter);
        }
    }

    public static void main(String[] args) throws Exception {
        new TestRunner<MapStoreTest>(new MapStoreTest()).withDuration(10).run();
    }
}
