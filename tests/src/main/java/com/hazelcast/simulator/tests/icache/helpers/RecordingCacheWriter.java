package com.hazelcast.simulator.tests.icache.helpers;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;

public class RecordingCacheWriter<K, V> implements CacheWriter<K, V>, Serializable {

    public ConcurrentHashMap<K, V> writtenKeys = new ConcurrentHashMap<K, V>();
    public ConcurrentHashMap<K, V> deletedEntries = new ConcurrentHashMap<K, V>();

    public AtomicLong writeCount = new AtomicLong();
    public AtomicLong deleteCount = new AtomicLong();

    public int writeDelayMs = 0;
    public int writeAllDelayMs = 0;
    public int deleteDelayMs = 0;
    public int deleteAllDelayMs = 0;

    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) {

        if (writeDelayMs > 0) {
            sleepMillis(writeDelayMs);
        }

        writtenKeys.put(entry.getKey(), entry.getValue());
        writeCount.incrementAndGet();
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {

        if (writeAllDelayMs > 0) {
            sleepMillis(writeAllDelayMs);
        }

        for (Cache.Entry<? extends K, ? extends V> entry : entries) {
            write(entry);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void delete(Object object) {

        if (deleteDelayMs > 0) {
            sleepMillis(deleteDelayMs);
        }

        K key = (K) object;
        V value = writtenKeys.remove(key);
        if (value != null) {
            deletedEntries.put(key, value);
        }
        deleteCount.incrementAndGet();
    }

    @Override
    public void deleteAll(Collection<?> entries) {

        if (deleteAllDelayMs > 0) {
            sleepMillis(deleteAllDelayMs);
        }

        for (Iterator<?> keys = entries.iterator(); keys.hasNext(); ) {
            delete(keys.next());
            keys.remove();
        }
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