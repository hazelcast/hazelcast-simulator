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
package com.hazelcast.simulator.tests.icache.helpers;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.EventType;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class ICacheEntryEventFilter<K, V> implements CacheEntryEventFilter<K, V>, Serializable {

    private final AtomicLong filtered = new AtomicLong();

    private EventType eventFilter;

    public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> event) {

        if (eventFilter != null && event.getEventType() == eventFilter) {
            filtered.incrementAndGet();
            return false;
        }
        return true;
    }

    public void setEventFilter(EventType eventFilter) {
        this.eventFilter = eventFilter;
    }

    @Override
    public String toString() {
        return "MyCacheEntryEventFilter{"
                + "eventFilter=" + eventFilter
                + ", filtered=" + filtered
                + '}';
    }
}
