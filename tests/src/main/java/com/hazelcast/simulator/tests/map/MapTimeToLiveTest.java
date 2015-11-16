/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapOperationCounter;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.util.EmptyStatement;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

/**
 * In this test we are using map put methods with an expire time.
 *
 * We put keys at random into the map using sync and async methods with some probability distribution.
 * In the end we verify that the map is empty and all key value pairs have expired out of the map.
 */
public class MapTimeToLiveTest {

    private static final ILogger LOGGER = Logger.getLogger(MapTimeToLiveTest.class);

    private enum Operation {
        PUT_TTL,
        ASYNC_PUT_TTL,
        GET,
        ASYNC_GET,
        DESTROY
    }

    // properties
    public String basename = MapTimeToLiveTest.class.getSimpleName();
    public int keyCount = 10;

    public double putTTLProb = 0.4;
    public double putAsyncTTLProb = 0.3;
    public double getProb = 0.2;
    public double getAsyncProb = 0.1;
    public double destroyProb = 0.0;

    public int maxTTLExpiryMs = 3000;
    public int minTTLExpiryMs = 1;

    private final OperationSelectorBuilder<Operation> builder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Integer> map;
    private IList<MapOperationCounter> results;

    @Setup
    public void setup(TestContext testContext) {
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        results = targetInstance.getList(basename + "report");

        builder.addOperation(Operation.PUT_TTL, putTTLProb)
                .addOperation(Operation.ASYNC_PUT_TTL, putAsyncTTLProb)
                .addOperation(Operation.GET, getProb)
                .addOperation(Operation.ASYNC_GET, getAsyncProb)
                .addOperation(Operation.DESTROY, destroyProb);
    }

    @Verify(global = true)
    public void globalVerify() {
        MapOperationCounter total = new MapOperationCounter();
        for (MapOperationCounter counter : results) {
            total.add(counter);
        }
        LOGGER.info(basename + ": " + total + " total of " + results.size());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(basename + ": Map should be empty, some TTL events are not processed", 0, map.size());
            }
        });
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private final MapOperationCounter count = new MapOperationCounter();

        public Worker() {
            super(builder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            try {
                int key = randomInt(keyCount);
                int value;
                int delayMs;

                switch (operation) {
                    case PUT_TTL:
                        value = randomInt();
                        delayMs = minTTLExpiryMs + randomInt(maxTTLExpiryMs);
                        map.put(key, value, delayMs, TimeUnit.MILLISECONDS);
                        count.putTTLCount.incrementAndGet();
                        break;
                    case ASYNC_PUT_TTL:
                        value = randomInt();
                        delayMs = minTTLExpiryMs + randomInt(maxTTLExpiryMs);
                        map.putAsync(key, value, delayMs, TimeUnit.MILLISECONDS);
                        count.putAsyncTTLCount.incrementAndGet();
                        break;
                    case GET:
                        map.get(key);
                        count.getCount.incrementAndGet();
                        break;
                    case ASYNC_GET:
                        map.getAsync(key);
                        count.getAsyncCount.incrementAndGet();
                        break;
                    case DESTROY:
                        map.destroy();
                        count.destroyCount.incrementAndGet();
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            } catch (DistributedObjectDestroyedException e) {
                EmptyStatement.ignore(e);
            }
        }

        @Override
        protected void afterRun() {
            results.add(count);
        }
    }

    public static void main(String[] args) throws Exception {
        new TestRunner<MapTimeToLiveTest>(new MapTimeToLiveTest()).run();
    }
}
