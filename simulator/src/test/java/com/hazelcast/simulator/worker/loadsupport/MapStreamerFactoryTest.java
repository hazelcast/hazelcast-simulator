package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.IMap;
import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MapStreamerFactoryTest {

    private final IMap map = mock(IMap.class);

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(MapStreamerFactory.class);
    }

    @Test
    public void testGetInstance() {
        MapStreamer streamer = MapStreamerFactory.getInstance(map);
        assertNotNull(streamer);
    }

    @Test
    public void testGetInstance_forceAsync() {
        MapStreamerFactory.enforceAsync(true);
        MapStreamer streamer = MapStreamerFactory.getInstance(map);
        assertNotNull(streamer);
        assertTrue(streamer instanceof AsyncMapStreamer);
    }

    @Test
    public void testGetInstance_forceSync() {
        MapStreamerFactory.enforceAsync(false);
        MapStreamer streamer = MapStreamerFactory.getInstance(map);
        assertNotNull(streamer);
        assertTrue(streamer instanceof SyncMapStreamer);
    }
}
