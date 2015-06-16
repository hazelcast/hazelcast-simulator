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
