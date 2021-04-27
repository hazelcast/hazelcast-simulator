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

import com.hazelcast.map.IMap;

/**
 * Synchronous implementation of {@link Streamer} for {@link IMap}.
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SyncMapStreamer<K, V> implements Streamer<K, V> {

    private final IMap<K, V> map;

    SyncMapStreamer(IMap<K, V> map) {
        this.map = map;
    }

    @Override
    public void pushEntry(K key, V value) {
        map.set(key, value);
    }

    @Override
    public void await() {
    }
}
