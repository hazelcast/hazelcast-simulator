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
package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.IMap;
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
