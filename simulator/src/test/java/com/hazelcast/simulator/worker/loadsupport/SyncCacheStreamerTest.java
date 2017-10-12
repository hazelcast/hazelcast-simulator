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

import com.hazelcast.util.EmptyStatement;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SyncCacheStreamerTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    @SuppressWarnings("unchecked")
    private final Cache<Integer, String> cache = mock(Cache.class);

    private Streamer<Integer, String> streamer;

    @Before
    public void before() {
        StreamerFactory.enforceAsync(false);
        streamer = StreamerFactory.getInstance(cache);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testPushEntry() {
        streamer.pushEntry(15, "value");

        verify(cache).put(15, "value");
        verifyNoMoreInteractions(cache);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testPushEntry_withException() {
        doThrow(new IllegalArgumentException()).when(cache).put(anyInt(), anyString());

        try {
            streamer.pushEntry(1, "foobar");
            fail("Expected exception directly thrown by pushEntry() method");
        } catch (Exception ignored) {
            EmptyStatement.ignore(ignored);
        }

        // this method should never block and never throw an exception
        streamer.await();

        verify(cache).put(1, "foobar");
        verifyNoMoreInteractions(cache);
    }
}
