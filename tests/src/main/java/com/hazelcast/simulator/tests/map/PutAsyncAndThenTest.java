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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;

public class PutAsyncAndThenTest {

    // properties
    public int keyCount = 1000;
    public int maxConcurrentCallsPerWorker = 1000;
    public String basename = PutAsyncAndThenTest.class.getSimpleName();
    public long acquireTimeoutMs = 2 * 60 * 1000;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private IMap<String, String> map;
    private TestContext context;
    private String[] keys;

    @Setup
    public void setUp(TestContext testContext) {
        this.context = testContext;
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        keys = generateStringKeys(basename, 10, keyLocality, testContext.getTargetInstance());

    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker implements ExecutionCallback {
        private final Semaphore semaphore = new Semaphore(maxConcurrentCallsPerWorker);

        @Override
        protected void timeStep() throws Exception {
            if (!semaphore.tryAcquire(1, acquireTimeoutMs, TimeUnit.MILLISECONDS)) {
                throw new RuntimeException("Failed to acquire a license from the semaphore within the given timeout");
            }

            String key = keys[randomInt(keyCount)];
            ICompletableFuture f = (ICompletableFuture) map.putAsync(key, "");
            f.andThen(this);
        }

        @Override
        public void onResponse(Object o) {
            semaphore.release();
        }

        @Override
        public void onFailure(Throwable throwable) {
            ExceptionReporter.report(context.getTestId(), throwable);
        }
    }
}
