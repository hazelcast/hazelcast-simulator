package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.logging.ILogger;
import org.junit.Test;

import static com.hazelcast.logging.Logger.getLogger;
import static com.hazelcast.simulator.tests.map.helpers.MapStoreUtils.assertMapStoreConfiguration;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;

public class MapStoreUtilsTest {

    private static final String MAP_NAME = "mapName";

    private static final ILogger LOGGER = getLogger(MapStoreUtils.class);

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(MapStoreUtils.class);
    }

    @Test
    public void testAssertMapStoreConfiguration_whenClient_thenReturn() {
        assertMapStoreConfiguration(LOGGER, null, MAP_NAME, MapStoreWithCounter.class);
    }
}
