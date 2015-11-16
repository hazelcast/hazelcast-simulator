package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.util.EmptyStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class AsyncMapStreamerTest {

    @SuppressWarnings("unchecked")
    private final IMap<Integer, String> map = mock(IMap.class);

    @SuppressWarnings("unchecked")
    private final ICompletableFuture<String> future = mock(ICompletableFuture.class);

    private Streamer<Integer, String> streamer;

    @Before
    public void setUp() {
        StreamerFactory.enforceAsync(true);
        streamer = StreamerFactory.getInstance(map);
    }

    @Test
    public void testPushEntry() {
        when(map.putAsync(anyInt(), anyString())).thenReturn(future);

        streamer.pushEntry(15, "value");

        verify(map).putAsync(15, "value");
        verifyNoMoreInteractions(map);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAwait() {
        when(map.putAsync(anyInt(), anyString())).thenReturn(future);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
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

    @Test(expected = IllegalArgumentException.class)
    @SuppressWarnings("unchecked")
    public void testAwait_withExceptionInFuture() {
        when(map.putAsync(anyInt(), anyString())).thenReturn(future);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
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

    @Test
    public void testAwait_withExceptionOnPushEntry() {
        doThrow(new IllegalArgumentException()).when(map).putAsync(anyInt(), anyString());

        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    streamer.pushEntry(1, "foobar");
                    fail("Expected exception directly thrown by pushEntry() method");
                } catch (Exception ignored) {
                    EmptyStatement.ignore(ignored);
                }
            }
        };
        thread.start();

        try {
            streamer.await();
        } finally {
            joinThread(thread);

            verify(map).putAsync(1, "foobar");
            verifyNoMoreInteractions(map);
        }
    }
}
