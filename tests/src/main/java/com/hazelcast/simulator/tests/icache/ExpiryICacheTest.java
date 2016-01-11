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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.utils.AssertTask;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import javax.cache.CacheManager;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;

public class ExpiryICacheTest {

    private static final ILogger LOGGER = Logger.getLogger(ExpiryICacheTest.class);

    //default keyCount entries of int,  is upper bound to approx 8MB possible max, if all put in side expiryPolicy time
    public int keyCount = 1000000;
    public String basename = ExpiryICacheTest.class.getSimpleName();

    private final ExpiryPolicy expiryPolicy = new CreatedExpiryPolicy(Duration.ONE_MINUTE);

    private ICache cache;

    @Setup
    public void setup(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();

        CacheManager cacheManager = createCacheManager(hazelcastInstance);
        cache = (ICache) cacheManager.getCache(basename);
    }

    @RunWithWorker
    public Worker run() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        @Override
        protected void beforeRun() {
        }

        @Override
        public void timeStep() {
            long key = getRandom().nextInt(keyCount);
            if (!cache.containsKey(key)) {
                cache.put(key, 0, expiryPolicy);
            }
        }

        @Override
        protected void afterRun() {
        }
    }

    @Verify(global = false)
    public void globalVerify() {

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                LOGGER.info(basename + " cache size = " + cache.size());
                assertEquals(basename + ": ICache should be empty, TTL events not processed", 0, cache.size());
            }
        });
    }

    public static void main(String[] args) throws Exception {
        new TestRunner<ExpiryICacheTest>(new ExpiryICacheTest()).run();
    }
}
