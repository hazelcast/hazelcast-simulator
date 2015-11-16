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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.cache.CacheManager;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;

/**
 * Demonstrates the effect of batching.
 *
 * It uses async methods to invoke operation and wait for future to complete every {@code batchSize} invocations.
 * Hence setting {@link #batchSize} to 1 is effectively the same as using sync operations.
 *
 * Setting {@link #batchSize} to values greater than 1 causes the batch-effect to kick-in, pipe-lines are utilized better
 * and overall throughput goes up.
 */
public class BatchingICacheTest {

    private enum Operation {
        PUT,
        GET,
    }

    // properties
    public int keyCount = 1000000;
    public String basename = BatchingICacheTest.class.getSimpleName();
    public double writeProb = 0.1;
    public int batchSize = 1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private ICache<Object, Object> cache;

    @Setup
    public void setup(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();

        CacheManager cacheManager = createCacheManager(hazelcastInstance);
        cache = (ICache<Object, Object>) cacheManager.getCache(basename);

        operationSelectorBuilder.addOperation(Operation.PUT, writeProb).addDefaultOperation(Operation.GET);
    }

    @Teardown
    public void teardown() {
        cache.close();
    }

    @Warmup(global = true)
    public void warmup() {
        Streamer<Object, Object> streamer = StreamerFactory.getInstance(cache);
        for (int i = 0; i < keyCount; i++) {
            streamer.pushEntry(i, 0);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private final List<ICompletableFuture<?>> futureList = new ArrayList<ICompletableFuture<?>>(batchSize);

        private long iteration;

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) throws Exception {
            Integer key = randomInt(keyCount);
            ICompletableFuture<?> future;
            switch (operation) {
                case PUT:
                    Integer value = randomInt();
                    future = cache.putAsync(key, value);
                    break;
                case GET:
                    future = cache.getAsync(key);
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown operation " + operation);
            }
            futureList.add(future);

            syncIfNecessary(iteration++);
        }

        private void syncIfNecessary(long iteration) throws Exception {
            if (iteration % batchSize == 0) {
                for (ICompletableFuture<?> future : futureList) {
                    future.get();
                }
                futureList.clear();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PerformanceICacheTest test = new PerformanceICacheTest();
        new TestRunner<PerformanceICacheTest>(test).run();
    }
}
