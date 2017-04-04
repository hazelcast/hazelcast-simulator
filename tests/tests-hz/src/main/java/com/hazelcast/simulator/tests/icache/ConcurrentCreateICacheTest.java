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
package com.hazelcast.simulator.tests.icache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.IList;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import java.io.Serializable;

import static com.hazelcast.simulator.tests.icache.helpers.CacheUtils.createCacheManager;
import static org.junit.Assert.assertEquals;

/**
 * In this test we concurrently call createCache. From multi clients/members we expect no exceptions
 * in the setup phase of this test. We count the number of {@link CacheException} thrown when creating the cache
 * from multi members and clients, and at verification we assert that no exceptions where thrown.
 */
public class ConcurrentCreateICacheTest extends HazelcastTest {

    private IList<Counter> counterList;

    @Setup
    public void setup() {
        counterList = targetInstance.getList(name);

        CacheConfig config = new CacheConfig();
        config.setName(name);

        Counter counter = new Counter();
        try {
            CacheManager cacheManager = createCacheManager(targetInstance);
            cacheManager.createCache(name, config);
            counter.create++;
        } catch (CacheException e) {
            logger.fatal(name + ": createCache exception " + e, e);
            counter.createException++;
        }
        counterList.add(counter);
    }

    @Run
    public void run() {
    }

    private static class Counter implements Serializable {

        public long create = 0;
        public long createException = 0;

        public void add(Counter counter) {
            create += counter.create;
            createException += counter.createException;
        }

        @Override
        public String toString() {
            return "Counter{"
                    + " create=" + create
                    + ", createException=" + createException
                    + '}';
        }
    }

    @Verify
    public void globalVerify() {
        Counter total = new Counter();
        for (Counter counter : counterList) {
            total.add(counter);
        }
        logger.info(name + ": " + total + " from " + counterList.size() + " worker threads");

        assertEquals(name + ": We expect 0 CacheException from multi node create cache calls", 0, total.createException);
    }
}
