package com.hazelcast.simulator.tests.icache.helpers;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class MyCacheEntryEventFilter<K, V> implements CacheEntryEventFilter<K, V>, Serializable {

    public EventType eventFilter = null;//EventType.CREATED;
    public final AtomicLong filtered = new AtomicLong();

    public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> event)  throws CacheEntryListenerException {

        if (eventFilter!=null && event.getEventType() == eventFilter){
            filtered.incrementAndGet();
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "MyCacheEntryEventFilter{" +
                "eventFilter=" + eventFilter +
                ", filtered=" + filtered +
                '}';
    }
}