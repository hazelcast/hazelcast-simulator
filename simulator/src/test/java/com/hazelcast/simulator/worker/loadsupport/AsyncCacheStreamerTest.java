package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.ICompletableFuture;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AsyncCacheStreamerTest {

    @SuppressWarnings("unchecked")
    private final ICache<Integer, String> cache = mock(ICache.class);

    @SuppressWarnings("unchecked")
    private final ICompletableFuture<Void> future = mock(ICompletableFuture.class);

    private Streamer<Integer, String> streamer;

    @Before
    public void setUp() {
        when(cache.putAsync(anyInt(), anyString())).thenReturn(future);

        StreamerFactory.enforceAsync(true);
        streamer = StreamerFactory.getInstance(cache);
    }

    @Test
    public void testPushEntry() {
        streamer.pushEntry(15, "value");

        verify(cache).putAsync(15, "value");
        verifyNoMoreInteractions(cache);
    }
}
