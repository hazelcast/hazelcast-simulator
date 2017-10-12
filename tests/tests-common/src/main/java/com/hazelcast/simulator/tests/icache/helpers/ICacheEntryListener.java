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
package com.hazelcast.simulator.tests.icache.helpers;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class ICacheEntryListener<K, V> implements CacheEntryCreatedListener<K, V>, CacheEntryRemovedListener<K, V>,
        CacheEntryUpdatedListener<K, V>, Serializable {

    private AtomicLong created = new AtomicLong();
    private AtomicLong updated = new AtomicLong();
    private AtomicLong removed = new AtomicLong();
    private AtomicLong expired = new AtomicLong();
    private AtomicLong unExpected = new AtomicLong();

    public long getUnexpected() {
        return unExpected.get();
    }

    public void add(ICacheEntryListener listener) {
        created.addAndGet(listener.created.get());
        updated.addAndGet(listener.updated.get());
        removed.addAndGet(listener.removed.get());
        expired.addAndGet(listener.expired.get());
        unExpected.addAndGet(listener.unExpected.get());
    }

    @Override
    public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
        for (CacheEntryEvent<? extends K, ? extends V> event : events) {
            switch (event.getEventType()) {
                case CREATED:
                    created.incrementAndGet();
                    break;
                default:
                    unExpected.incrementAndGet();
                    break;
            }
        }
    }

    @Override
    public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
        for (CacheEntryEvent<? extends K, ? extends V> event : events) {
            switch (event.getEventType()) {
                case REMOVED:
                    removed.incrementAndGet();
                    break;
                default:
                    unExpected.incrementAndGet();
                    break;
            }
        }
    }

    @Override
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) {
        for (CacheEntryEvent<? extends K, ? extends V> event : events) {
            switch (event.getEventType()) {
                case UPDATED:
                    updated.incrementAndGet();
                    break;
                default:
                    unExpected.incrementAndGet();
                    break;
            }
        }
    }

    @Override
    public String toString() {
        return "MyCacheEntryListener{"
                + "created=" + created
                + ", updated=" + updated
                + ", removed=" + removed
                + ", expired=" + expired
                + ", unExpected=" + unExpected
                + '}';
    }
}
