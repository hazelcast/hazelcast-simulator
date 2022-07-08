package com.hazelcast.simulator.tests.map.helpers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;


import static com.hazelcast.simulator.tests.map.helpers.MapStoreUtils.assertMapStoreConfiguration;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class MapStoreUtilsTest {

    private static final String MAP_NAME = "mapName";

    private static final Logger LOGGER = LogManager.getLogger(MapStoreUtils.class);

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(MapStoreUtils.class);
    }

    @Test
    public void testAssertMapStoreConfiguration_whenClient_thenReturn() {
        assertMapStoreConfiguration(LOGGER, null, MAP_NAME, MapStoreWithCounter.class);
    }
}
