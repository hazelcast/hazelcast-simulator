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

import javax.cache.Cache;

/**
 * Synchronous implementation of {@link Streamer} for {@link Cache}.
 *
 * @param <K> key type
 * @param <V> value type
 */
class SyncCacheStreamer<K, V> implements Streamer<K, V> {

    private final Cache<K, V> cache;

    SyncCacheStreamer(Cache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public void pushEntry(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public void await() {
    }
}
