package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cache.ICache;
import com.hazelcast.map.IMap;
import org.junit.Test;

import javax.cache.Cache;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class StreamerFactoryTest {

    private final IMap iMap = mock(IMap.class);
    private final ICache iCache = mock(ICache.class);
    private final Cache cache = mock(Cache.class);

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(StreamerFactory.class);
    }

    @Test
    public void testGetInstance_withMap() {
        Streamer streamer = StreamerFactory.getInstance(iMap);
        assertNotNull(streamer);
    }

    @Test
    public void testGetInstance_withMap_forceAsync() {
        StreamerFactory.enforceAsync(true);
        Streamer streamer = StreamerFactory.getInstance(iMap);
        assertNotNull(streamer);
        assertTrue(streamer instanceof AsyncMapStreamer);
    }

    @Test
    public void testGetInstance_withMap_forceSync() {
        StreamerFactory.enforceAsync(false);
        Streamer streamer = StreamerFactory.getInstance(iMap);
        assertNotNull(streamer);
        assertTrue(streamer instanceof SyncMapStreamer);
    }

    @Test
    public void testGetInstance_withICache() {
        Streamer streamer = StreamerFactory.getInstance(iCache);
        assertNotNull(streamer);
    }

    @Test
    public void testGetInstance_withICache_forceAsync() {
        StreamerFactory.enforceAsync(true);
        Streamer streamer = StreamerFactory.getInstance(iCache);
        assertNotNull(streamer);
        assertTrue(streamer instanceof AsyncCacheStreamer);
    }

    @Test
    public void testGetInstance_withICache_forceSync() {
        StreamerFactory.enforceAsync(false);
        Streamer streamer = StreamerFactory.getInstance(iCache);
        assertNotNull(streamer);
        assertTrue(streamer instanceof SyncCacheStreamer);
    }

    @Test
    public void testGetInstance_withCache() {
        Streamer streamer = StreamerFactory.getInstance(cache);
        assertNotNull(streamer);
    }

    @Test
    public void testGetInstance_withCache_forceAsync() {
        StreamerFactory.enforceAsync(true);
        Streamer streamer = StreamerFactory.getInstance(cache);
        assertNotNull(streamer);
        assertTrue(streamer instanceof SyncCacheStreamer);
    }

    @Test
    public void testGetInstance_withCache_forceSync() {
        StreamerFactory.enforceAsync(false);
        Streamer streamer = StreamerFactory.getInstance(cache);
        assertNotNull(streamer);
        assertTrue(streamer instanceof SyncCacheStreamer);
    }
}
