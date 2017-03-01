package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This is needed when Simulator is compiled against Hazelcast 3.7+, but uses 3.6- in runtime.
 *
 * Reflection is not ideal, but it's still way better than using {@link SyncMapStreamer}
 *
 * @param <K>
 * @param <V>
 */
public class ReflectionAsyncMapStreamer<K, V> extends AbstractAsyncStreamer<K, V> {

    private static final Method PUT_ASYNC_METHOD;
    private final IMap<K, V> map;

    static {
        try {
            PUT_ASYNC_METHOD = IMap.class.getMethod("putAsync", Object.class, Object.class);
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    ReflectionAsyncMapStreamer(int concurrencyLevel, IMap<K, V> map) {
        super(concurrencyLevel);
        this.map = map;
    }

    @Override
    ICompletableFuture storeAsync(K key, V value) {
        try {
            return (ICompletableFuture) PUT_ASYNC_METHOD.invoke(map, key, value);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        } catch (InvocationTargetException e) {
            throw new AssertionError(e);
        }
    }
}
