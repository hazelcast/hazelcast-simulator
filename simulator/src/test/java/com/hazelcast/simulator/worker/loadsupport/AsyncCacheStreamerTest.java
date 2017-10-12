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
package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AsyncCacheStreamerTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    @SuppressWarnings("unchecked")
    private final ICache<Integer, String> cache = mock(ICache.class);

    @SuppressWarnings("unchecked")
    private final ICompletableFuture<Void> future = mock(ICompletableFuture.class);

    private Streamer<Integer, String> streamer;

    @Before
    public void before() {
        setupFakeUserDir();

        when(cache.putAsync(anyInt(), anyString())).thenReturn(future);

        StreamerFactory.enforceAsync(true);
        streamer = StreamerFactory.getInstance(cache);
    }

    @After
    public void after() {
        teardownFakeUserDir();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testPushEntry() {
        streamer.pushEntry(15, "value");

        verify(cache).putAsync(15, "value");
        verifyNoMoreInteractions(cache);
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    @SuppressWarnings("unchecked")
    public void testAwait() {
        when(cache.putAsync(anyInt(), anyString())).thenReturn(future);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Object[] arguments = invocation.getArguments();
                ExecutionCallback<String> callback = (ExecutionCallback<String>) arguments[0];

                callback.onResponse("value");
                return null;
            }
        }).when(future).andThen(any(ExecutionCallback.class));

        Thread thread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 5000; i++) {
                    streamer.pushEntry(i, "value");
                }
            }
        };
        thread.start();

        streamer.await();
        joinThread(thread);
    }

    @Test(timeout = DEFAULT_TIMEOUT, expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void testAwait_withExceptionInFuture() {
        when(cache.putAsync(anyInt(), anyString())).thenReturn(future);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) {
                Object[] arguments = invocation.getArguments();
                ExecutionCallback<String> callback = (ExecutionCallback<String>) arguments[0];

                Exception exception = new IllegalArgumentException("expected exception");
                callback.onFailure(exception);
                return null;
            }
        }).when(future).andThen(any(ExecutionCallback.class));

        streamer.pushEntry(1, "value");
        streamer.await();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testAwait_withExceptionOnPushEntry() throws Exception {
        doThrow(new IllegalArgumentException("expected exception")).when(cache).putAsync(anyInt(), anyString());
        final CountDownLatch latch = new CountDownLatch(1);

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    streamer.pushEntry(1, "foobar");
                    fail("Expected exception directly thrown by pushEntry() method");
                } catch (Exception ignored) {
                    latch.countDown();
                }
            }
        };
        thread.start();

        try {
            latch.await();
            streamer.await();
        } finally {
            joinThread(thread);

            verify(cache).putAsync(1, "foobar");
            verifyNoMoreInteractions(cache);
        }
    }

    @Test(timeout = DEFAULT_TIMEOUT, expected = CommandLineExitException.class)
    public void testPushEntry_withInterruptedSemaphore() throws Exception {
        Semaphore semaphore = mock(Semaphore.class);
        when(semaphore.tryAcquire(anyInt(), anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("expected"));

        streamer = new AsyncCacheStreamer<Integer, String>(1, cache, semaphore);

        streamer.pushEntry(1, "test");
    }

    @Test(timeout = DEFAULT_TIMEOUT, expected = IllegalStateException.class)
    public void testPushEntry_withSemaphoreTimeout() throws Exception {
        Semaphore semaphore = mock(Semaphore.class);
        when(semaphore.tryAcquire(anyInt(), anyLong(), any(TimeUnit.class))).thenReturn(false);

        streamer = new AsyncCacheStreamer<Integer, String>(1, cache, semaphore);

        streamer.pushEntry(1, "test");
    }
}
