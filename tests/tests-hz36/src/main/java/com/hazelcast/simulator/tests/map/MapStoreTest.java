/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.AbstractTest;
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
import static com.hazelcast.simulator.tests.map.helpers.MapStoreUtils.assertMapStoreConfiguration;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This test operates on a map which has a {@link com.hazelcast.core.MapStore} configured.
 *
 * We use map operations such as loadAll, put, get, delete or destroy with some probability distribution to trigger
 * {@link com.hazelcast.core.MapStore} methods. We verify that the the key/value pairs in the map are also "persisted"
 * into the {@link com.hazelcast.core.MapStore}.
 */
public class MapStoreTest extends AbstractTest {

    private enum MapOperation {
        LOAD_ALL,
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

    public int keyCount = 10;

    public double loadAllProb = 0.1;
    public double putProb = 0.4;
    public double getProb = 0.2;
    public double getAsyncProb = 0.2;
    public double deleteProb = 0.1;
    public double destroyProb = 0.0;

    public double putUsingPutProb = 0.4;
    public double putUsingPutAsyncProb = 0.0;
    public double putUsingPutTTLProb = 0.3;
    public double putUsingPutIfAbsent = 0.15;
    public double putUsingReplaceProb = 0.15;

    public int mapStoreMaxDelayMs = 0;
    public int mapStoreMinDelayMs = 0;

    public int maxTTLExpiryMs = 3000;
    public int minTTLExpiryMs = 100;

    private final OperationSelectorBuilder<MapOperation> mapOperationBuilder = new OperationSelectorBuilder<MapOperation>();
    private final OperationSelectorBuilder<MapPutOperation> putOperationBuilder = new OperationSelectorBuilder<MapPutOperation>();

    private int putTTlKeyDomain;
    private int putTTlKeyRange;

    private IMap<Integer, Integer> map;
    private IList<MapOperationCounter> operationCounterList;

    @Setup
    public void setup() {
        putTTlKeyDomain = keyCount;
        putTTlKeyRange = keyCount;

        map = targetInstance.getMap(name);
        operationCounterList = targetInstance.getList(name + "report");

        MapStoreWithCounter.setMinMaxDelayMs(mapStoreMinDelayMs, mapStoreMaxDelayMs);

        mapOperationBuilder
                .addOperation(MapOperation.LOAD_ALL, loadAllProb)
                .addOperation(MapOperation.PUT, putProb)
                .addOperation(MapOperation.GET, getProb)
                .addOperation(MapOperation.GET_ASYNC, getAsyncProb)
                .addOperation(MapOperation.DELETE, deleteProb)
                .addOperation(MapOperation.DESTROY, destroyProb);

        putOperationBuilder
                .addOperation(MapPutOperation.PUT, putUsingPutProb)
                .addOperation(MapPutOperation.PUT_ASYNC, putUsingPutAsyncProb)
                .addOperation(MapPutOperation.PUT_TTL, putUsingPutTTLProb)
                .addOperation(MapPutOperation.PUT_IF_ABSENT, putUsingPutIfAbsent)
                .addOperation(MapPutOperation.REPLACE, putUsingReplaceProb);

        assertMapStoreConfiguration(logger, targetInstance, name, MapStoreWithCounter.class);
    }



    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<MapOperation> {

        private final MapOperationCounter operationCounter = new MapOperationCounter();
        private final OperationSelector<MapPutOperation> putOperationSelector = putOperationBuilder.build();

        public Worker() {
            super(mapOperationBuilder);
        }

        @Override
        public void timeStep(MapOperation mapOperation) {
            Integer key = randomInt(keyCount);

            switch (mapOperation) {
                case LOAD_ALL:
                    map.loadAll(true);
                    break;

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

            switch (putOperationSelector.select()) {
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
        public void afterRun() {
            operationCounterList.add(operationCounter);
        }
    }

    @Verify
    public void globalVerify() {
        MapOperationCounter total = new MapOperationCounter();
        for (MapOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        logger.info(name + ": " + total + " from " + operationCounterList.size() + " worker threads");
    }

    @Verify(global = false)
    public void verify() {
        if (isClient(targetInstance)) {
            return;
        }

        MapStoreConfig mapStoreConfig = targetInstance.getConfig().getMapConfig(name).getMapStoreConfig();
        int writeDelayMs = (int) TimeUnit.SECONDS.toMillis(mapStoreConfig.getWriteDelaySeconds());

        int sleepMs = mapStoreMaxDelayMs * 2 + maxTTLExpiryMs * 2 + (writeDelayMs * 2);
        logger.info("Sleeping for " + TimeUnit.MILLISECONDS.toSeconds(sleepMs) + " seconds to wait for delay and TTL values.");
        sleepMillis(sleepMs);

        final MapStoreWithCounter mapStore = (MapStoreWithCounter) mapStoreConfig.getImplementation();

        logger.info(name + ": map size = " + map.size());
        logger.info(name + ": map store = " + mapStore);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                for (Integer key : map.localKeySet()) {
                    assertEquals(map.get(key), mapStore.get(key));
                }
                assertEquals("Map entrySets should be equal", map.getAll(map.localKeySet()).entrySet(), mapStore.entrySet());

                for (int key = putTTlKeyDomain; key < putTTlKeyDomain + putTTlKeyRange; key++) {
                    assertNull(name + ": TTL key should not be in the map", map.get(key));
                }
            }
        });
    }
}
