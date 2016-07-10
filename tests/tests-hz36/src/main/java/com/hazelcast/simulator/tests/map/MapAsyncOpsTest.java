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
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class MapAsyncOpsTest extends AbstractTest {

    private enum Operation {
        PUT_ASYNC,
        PUT_ASYNC_TTL,
        GET_ASYNC,
        REMOVE_ASYNC,
        DESTROY
    }

    // properties
    public int keyCount = 10;
    public int maxTTLExpirySeconds = 3;

    public double putAsyncProb = 0.2;
    public double putAsyncTTLProb = 0.2;
    public double getAsyncProb = 0.2;
    public double removeAsyncProb = 0.2;
    public double destroyProb = 0.2;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private final MapOperationCounter count = new MapOperationCounter();

    private IMap<Integer, Object> map;
    private IList<MapOperationCounter> results;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        results = targetInstance.getList(name + "report");

        operationSelectorBuilder.addOperation(Operation.PUT_ASYNC, putAsyncProb)
                .addOperation(Operation.PUT_ASYNC_TTL, putAsyncTTLProb)
                .addOperation(Operation.GET_ASYNC, getAsyncProb)
                .addOperation(Operation.REMOVE_ASYNC, removeAsyncProb)
                .addOperation(Operation.DESTROY, destroyProb);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {
        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            int key = randomInt(keyCount);
            switch (operation) {
                case PUT_ASYNC:
                    Object value = randomInt();
                    map.putAsync(key, value);
                    count.putAsyncCount.incrementAndGet();
                    break;
                case PUT_ASYNC_TTL:
                    value = randomInt();
                    int delay = 1 + randomInt(maxTTLExpirySeconds);
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
                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        public void afterRun() {
            results.add(count);
        }
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
