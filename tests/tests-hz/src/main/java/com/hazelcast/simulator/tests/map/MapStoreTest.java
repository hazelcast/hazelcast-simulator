/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.IList;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;
import com.hazelcast.simulator.tests.map.helpers.MapStoreWithCounter;
import com.hazelcast.simulator.utils.AssertTask;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.map.helpers.MapStoreUtils.assertMapStoreConfiguration;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * This test operates on a map which has a {@link com.hazelcast.map.MapStore} configured.
 *
 * We use map operations such as loadAll, put, get, delete or destroy with some probability distribution to trigger
 * {@link com.hazelcast.map.MapStore} methods. We verify that the the key/value pairs in the map are also "persisted"
 * into the {@link com.hazelcast.map.MapStore}.
 */
public class MapStoreTest extends HazelcastTest {

    public int keyCount = 10;

    public int mapStoreMaxDelayMs = 0;
    public int mapStoreMinDelayMs = 0;

    public int maxTTLExpiryMs = 3000;
    public int minTTLExpiryMs = 100;

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

        assertMapStoreConfiguration(logger, targetInstance, name, MapStoreWithCounter.class);
    }

    @TimeStep(prob = 0.1)
    public void loadAll(ThreadState state) {
        map.loadAll(true);
    }

    @TimeStep(prob = 0.2)
    public void put(ThreadState state) {
        Integer key = state.randomKey();
        map.put(key, state.randomValue());
        state.operationCounter.putCount.incrementAndGet();
    }

    @TimeStep(prob = 0)
    public void putAsync(ThreadState state) {
        Integer key = state.randomKey();
        map.putAsync(key, state.randomValue());
        state.operationCounter.putAsyncCount.incrementAndGet();
    }

    @TimeStep(prob = 0.15)
    public void putTTL(ThreadState state) {
        Integer key = state.randomKey();
        int delayKey = putTTlKeyDomain + state.randomInt(putTTlKeyRange);
        int delayMs = minTTLExpiryMs + state.randomInt(maxTTLExpiryMs);
        map.putTransient(delayKey, state.randomValue(), delayMs, TimeUnit.MILLISECONDS);
        state.operationCounter.putTransientCount.incrementAndGet();
    }

    @TimeStep(prob = 0.075)
    public void putIfAbsent(ThreadState state) {
        Integer key = state.randomKey();
        map.putIfAbsent(key, state.randomValue());
        state.operationCounter.putIfAbsentCount.incrementAndGet();
    }

    @TimeStep(prob = 0.075)
    public void replace(ThreadState state) {
        Integer key = state.randomKey();
        Integer orig = map.get(key);
        if (orig != null) {
            map.replace(key, orig, state.randomValue());
            state.operationCounter.replaceCount.incrementAndGet();
        }
    }

    @TimeStep(prob = 0.2)
    public void get(ThreadState state) {
        Integer key = state.randomKey();
        map.get(key);
        state.operationCounter.getCount.incrementAndGet();
    }

    @TimeStep(prob = 0.2)
    public void getAsync(ThreadState state) {
        Integer key = state.randomKey();
        map.getAsync(key);
        state.operationCounter.getAsyncCount.incrementAndGet();
    }

    @TimeStep(prob = 0.1)
    public void delete(ThreadState state) {
        Integer key = state.randomKey();
        map.delete(key);
        state.operationCounter.deleteCount.incrementAndGet();
    }

    @TimeStep(prob = 0)
    public void destroy(ThreadState state) {
        map.destroy();
        state.operationCounter.destroyCount.incrementAndGet();
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounterList.add(state.operationCounter);
    }

    public class ThreadState extends BaseThreadState {

        final MapOperationCounter operationCounter = new MapOperationCounter();

        Integer randomKey() {
            return randomInt(keyCount);
        }

        Integer randomValue() {
            return randomInt();
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
