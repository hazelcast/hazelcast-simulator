package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.IMap;
import org.junit.Test;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StreamerFactoryTest {

    private final IMap map = mock(IMap.class);

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(StreamerFactory.class);
    }

    @Test
    public void testGetInstance() {
        Streamer streamer = StreamerFactory.getInstance(map);
        assertNotNull(streamer);
    }

    @Test
    public void testGetInstance_forceAsync() {
        StreamerFactory.enforceAsync(true);
        Streamer streamer = StreamerFactory.getInstance(map);
        assertNotNull(streamer);
        assertTrue(streamer instanceof AsyncMapStreamer);
    }

    @Test
    public void testGetInstance_forceSync() {
        StreamerFactory.enforceAsync(false);
        Streamer streamer = StreamerFactory.getInstance(map);
        assertNotNull(streamer);
        assertTrue(streamer instanceof SyncMapStreamer);
    }
}
