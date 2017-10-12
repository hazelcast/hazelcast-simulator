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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapStoreWithCounterPerKey;

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
        map = targetInstance.getMap(name);
        keyCounter = targetInstance.getAtomicLong(name);

        assertMapStoreConfiguration(logger, targetInstance, name, MapStoreWithCounterPerKey.class);
    }

    @TimeStep
    public void timeStep() {
        long key = keyCounter.incrementAndGet();
        map.put(key, "test value");
    }

    @Verify(global = false)
    public void verify() {
        if (isClient(targetInstance)) {
            return;
        }

        MapConfig mapConfig = targetInstance.getConfig().getMapConfig(name);
        logger.info(name + ": MapConfig: " + mapConfig);

        MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
        logger.info(name + ": MapStoreConfig: " + mapStoreConfig);

        int sleepSeconds = mapConfig.getTimeToLiveSeconds() * 2 + mapStoreConfig.getWriteDelaySeconds() * 2;
        logger.info("Sleeping for " + sleepSeconds + " seconds to wait for delay and TTL values.");
        sleepSeconds(sleepSeconds);

        MapStoreWithCounterPerKey mapStore = (MapStoreWithCounterPerKey) mapStoreConfig.getImplementation();
        logger.info(name + ": map size = " + map.size());
        logger.info(name + ": map store = " + mapStore);

        logger.info(name + ": Checking if some keys where stored more than once");
        for (Object key : mapStore.keySet()) {
            assertEquals("There were multiple calls to MapStore.store", 1, mapStore.valueOf(key));
        }
    }

}
