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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.AbstractTest;
import com.hazelcast.simulator.tests.map.helpers.MapStoreWithCounterPerKey;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.map.helpers.MapStoreUtils.assertMapStoreConfiguration;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

/**
 * This test checks for duplicate store entries when eviction happens and MapStore is slow (see Hazelcast issue #4448).
 */
public class MapEvictAndStoreTest extends AbstractTest {

    private IMap<Long, String> map;
    private IAtomicLong keyCounter;

    @Setup
    public void setup() {
        map = targetInstance.getMap(basename);
        keyCounter = targetInstance.getAtomicLong(basename);

        assertMapStoreConfiguration(logger, targetInstance, basename, MapStoreWithCounterPerKey.class);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    public class Worker extends AbstractMonotonicWorker {

        @Override
        public void timeStep() {
            long key = keyCounter.incrementAndGet();
            map.put(key, "test value");
        }
    }

    @Verify(global = false)
    public void verify() {
        if (isClient(targetInstance)) {
            return;
        }

        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(basename);
        logger.info(basename + ": MapConfig: " + mapConfig);

        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        logger.info(basename + ": MapStoreConfig: " + mapStoreConfig);

        int sleepSeconds = mapConfig.getTimeToLiveSeconds() * 2 + mapStoreConfig.getWriteDelaySeconds() * 2;
        logger.info("Sleeping for " + sleepSeconds + " seconds to wait for delay and TTL values.");
        sleepSeconds(sleepSeconds);

        MapStoreWithCounterPerKey mapStore = (MapStoreWithCounterPerKey) mapStoreConfig.getImplementation();
        logger.info(basename + ": map size = " + map.size());
        logger.info(basename + ": map store = " + mapStore);

        logger.info(basename + ": Checking if some keys where stored more than once");
        for (Object key : mapStore.keySet()) {
            assertEquals("There were multiple calls to MapStore.store", 1, mapStore.valueOf(key));
        }
    }

}
