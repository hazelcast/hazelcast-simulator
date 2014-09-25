package com.hazelcast.stabilizer.tests.icache.helpers;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class RecordingCacheWriter<K, V> implements CacheWriter<K, V>, Serializable {

    public ConcurrentHashMap<K, V> writtenKeys = new ConcurrentHashMap();
    public ConcurrentHashMap<K, V> deletedEntries = new ConcurrentHashMap();

    public AtomicLong writeCount =  new AtomicLong();
    public AtomicLong deleteCount =  new AtomicLong();

    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) {
        writtenKeys.put(entry.getKey(), entry.getValue());
        writeCount.incrementAndGet();
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {
        Iterator<Cache.Entry<? extends K, ? extends V>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            write(iterator.next());
        }
    }

    @Override
    public void delete(Object key) {
        V value = writtenKeys.remove((K)key);
        if (value != null) {
            deletedEntries.put((K) key, value);
        }
        deleteCount.incrementAndGet();
    }

    @Override
    public void deleteAll(Collection<?> entries) {
        for (Iterator<?> keys = entries.iterator(); keys.hasNext(); ) {
            delete(keys.next());
            keys.remove();
        }
    }

    public V get(K key) {
        return writtenKeys.get(key);
    }

    public boolean hasWritten(K key) {
        return writtenKeys.containsKey(key);
    }

    public boolean hasDeleted(K key) {
        return deletedEntries.containsKey(key);
    }

    @Override
    public String toString() {
        return "RecordingCacheWriter{" +
                "writtenKeys=" + writtenKeys +
                ", deletedEntries=" + deletedEntries +
                ", writeCount=" + writeCount +
                ", deleteCount=" + deleteCount +
                '}';
    }
}