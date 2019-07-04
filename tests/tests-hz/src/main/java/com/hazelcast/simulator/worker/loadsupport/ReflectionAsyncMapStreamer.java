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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.map.IMap;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * This is needed when Simulator is compiled against Hazelcast 3.7+, but uses 3.6- in runtime.
 *
 * Reflection is not ideal, but it's still way better than using {@link SyncMapStreamer}
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
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
