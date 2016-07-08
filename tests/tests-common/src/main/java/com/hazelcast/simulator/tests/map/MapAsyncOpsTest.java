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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.BaseThreadContext;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class MapAsyncOpsTest extends AbstractTest {

    // properties
    public int keyCount = 10;
    public int maxTTLExpirySeconds = 3;

    private final MapOperationCounter count = new MapOperationCounter();

    private IMap<Integer, Object> map;
    private IList<MapOperationCounter> results;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        results = targetInstance.getList(name + "report");
    }

    @TimeStep(prob = 0.2)
    public void putAsync(BaseThreadContext context) {
        int key = context.randomInt(keyCount);
        Object value = context.randomInt();
        map.putAsync(key, value);
        count.putAsyncCount.incrementAndGet();
    }

    @TimeStep(prob = 0.2)
    public void putAsyncTTL(BaseThreadContext context) {
        int key = context.randomInt(keyCount);
        int value = context.randomInt();
        int delay = 1 + context.randomInt(maxTTLExpirySeconds);
        map.putAsync(key, value, delay, TimeUnit.SECONDS);
        count.putAsyncTTLCount.incrementAndGet();
    }

    @TimeStep(prob = 0.2)
    public void getAsync(BaseThreadContext context) {
        int key = context.randomInt(keyCount);
        map.getAsync(key);
        count.getAsyncCount.incrementAndGet();
    }

    @TimeStep(prob = 0.2)
    public void removeAsync(BaseThreadContext context) {
        int key = context.randomInt(keyCount);
        map.removeAsync(key);
        count.removeAsyncCount.incrementAndGet();
    }

    @TimeStep(prob = 0.2)
    public void destroy() {
        map.destroy();
        count.destroyCount.incrementAndGet();
    }

    @AfterRun
    public void afterRun() {
        results.add(count);
    }

    @Verify
    public void globalVerify() {
        MapOperationCounter totalMapOperationsCount = new MapOperationCounter();
        for (MapOperationCounter mapOperationsCount : results) {
            totalMapOperationsCount.add(mapOperationsCount);
        }
        logger.info(name + ": " + totalMapOperationsCount + " total of " + results.size());
    }

    @Verify(global = false)
    public void verify() {
        sleepSeconds(maxTTLExpirySeconds * 2);

        logger.info(name + ": map size  =" + map.size());
    }
}
