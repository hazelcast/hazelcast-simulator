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
package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapStore;
import com.hazelcast.simulator.test.TestException;
import org.apache.log4j.Logger;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static java.lang.String.format;

public final class MapStoreUtils {

    private MapStoreUtils() {
    }

    public static void assertMapStoreConfiguration(Logger logger, HazelcastInstance instance, String mapName,
                                                   Class<? extends MapStore> mapStoreImplementation) {
        if (isClient(instance)) {
            return;
        }
        String expectedMapStoreName = mapStoreImplementation.getName();
        MapStoreConfig mapStoreConfig = instance.getConfig().getMapConfig(mapName).getMapStoreConfig();
        assertMapStoreConfig(expectedMapStoreName, mapName, mapStoreConfig, logger);
        assertMapStoreClassName(expectedMapStoreName, mapName, mapStoreConfig);
        assertMapStoreImplementation(expectedMapStoreName, mapName, mapStoreConfig, mapStoreImplementation);
    }

    private static void assertMapStoreConfig(String expectedMapStoreName, String mapName, MapStoreConfig mapStoreConfig,
                                             Logger logger) {
        if (mapStoreConfig == null) {
            throw new TestException("MapStore for map %s needs to be configured with class %s, but was not configured at all",
                    mapName, expectedMapStoreName);
        }
        logger.info(format("MapStore configuration for map %s: %s", mapName, mapStoreConfig));
        if (!mapStoreConfig.isEnabled()) {
            throw new TestException("MapStore for map %s needs to be configured with class %s, but was not enabled", mapName,
                    expectedMapStoreName);
        }
    }

    private static void assertMapStoreClassName(String expectedMapStoreName, String mapName, MapStoreConfig mapStoreConfig) {
        String configuredMapStoreClassName = mapStoreConfig.getClassName();
        if (configuredMapStoreClassName == null) {
            throw new TestException("MapStore for map %s needs to be configured with class %s, but was null", mapName,
                    expectedMapStoreName);
        }
        if (!expectedMapStoreName.equals(configuredMapStoreClassName)) {
            throw new TestException("MapStore for map %s needs to be configured with class %s, but was %s", mapName,
                    expectedMapStoreName, configuredMapStoreClassName);
        }
    }

    private static void assertMapStoreImplementation(String expectedMapStoreName, String mapName, MapStoreConfig mapStoreConfig,
                                                     Class<? extends MapStore> mapStoreImplementation) {
        Object configuredMapStoreImpl = mapStoreConfig.getImplementation();
        if (configuredMapStoreImpl == null) {
            if (mapStoreConfig.getInitialLoadMode().equals(LAZY)) {
                return;
            }
            throw new TestException("MapStore for map %s needs to be initialized with class %s, but was null (%s)", mapName,
                    expectedMapStoreName, mapStoreConfig);
        }
        if (!configuredMapStoreImpl.getClass().equals(mapStoreImplementation)) {
            throw new TestException("MapStore for map %s needs to be initialized with class %s, but was %s (%s)", mapName,
                    expectedMapStoreName, configuredMapStoreImpl.getClass().getName(), mapStoreConfig);
        }
    }
}
