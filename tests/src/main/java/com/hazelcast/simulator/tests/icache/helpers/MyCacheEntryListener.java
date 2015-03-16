package com.hazelcast.simulator.tests.icache.helpers;

import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class MyCacheEntryListener<K, V> implements CacheEntryCreatedListener<K, V>, CacheEntryRemovedListener<K, V>, CacheEntryUpdatedListener<K, V>, Serializable {

    public AtomicLong created = new AtomicLong();
    public AtomicLong updated = new AtomicLong();
    public AtomicLong removed = new AtomicLong();
    public AtomicLong expired = new AtomicLong();
    public AtomicLong unExpected = new AtomicLong();

    public void onCreated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {

        for (CacheEntryEvent<? extends K, ? extends V> event : events) {
            switch (event.getEventType()){
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
    public void onRemoved(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {

        for (CacheEntryEvent<? extends K, ? extends V> event : events) {
            switch (event.getEventType()){
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
    public void onUpdated(Iterable<CacheEntryEvent<? extends K, ? extends V>> events) throws CacheEntryListenerException {

        for (CacheEntryEvent<? extends K, ? extends V> event : events) {
            switch (event.getEventType()){
                case UPDATED:
                    updated.incrementAndGet();
                    break;
                default:
                    unExpected.incrementAndGet();
                    break;
            }
        }
    }

    public String toString() {
        return "MyCacheEntryListener{" +
                "created=" + created +
                ", updated=" + updated +
                ", removed=" + removed +
                ", expired=" + expired +
                ", unExpected=" + unExpected +
                '}';
    }

    public void add(MyCacheEntryListener listener){
        created.addAndGet(listener.created.get());
        updated.addAndGet(listener.updated.get());
        removed.addAndGet( listener.removed.get() );
        expired.addAndGet(listener.expired.get());
        unExpected.addAndGet( listener.unExpected.get() );
    }
}

