package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AsyncMapStreamerTest {

    @SuppressWarnings("unchecked")
    private final IMap<Integer, String> map = mock(IMap.class);

    @SuppressWarnings("unchecked")
    private final ICompletableFuture<String> future = mock(ICompletableFuture.class);

    private MapStreamer<Integer, String> streamer;

    @Before
    public void setUp() {
        when(map.putAsync(anyInt(), anyString())).thenReturn(future);

        MapStreamerFactory.enforceAsync(true);
        streamer = MapStreamerFactory.getInstance(map);
    }

    @Test
    public void testPushEntry() {
        streamer.pushEntry(15, "value");

        verify(map).putAsync(15, "value");
        verifyNoMoreInteractions(map);
    }
}
