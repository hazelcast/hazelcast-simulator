package com.hazelcast.stabilizer.tests.icache.helpers;

import javax.cache.Cache;
import javax.cache.integration.CacheWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.sleepMs;

public class RecordingCacheWriter<K, V> implements CacheWriter<K, V>, Serializable {

    public ConcurrentHashMap<K, V> writtenKeys = new ConcurrentHashMap();
    public ConcurrentHashMap<K, V> deletedEntries = new ConcurrentHashMap();

    public AtomicLong writeCount =  new AtomicLong();
    public AtomicLong deleteCount =  new AtomicLong();

    public int writeDelayMs = 0;
    public int writeAllDelayMs =0;
    public int deleteDelayMs = 0;
    public int deleteAllDelayMs =0;

    @Override
    public void write(Cache.Entry<? extends K, ? extends V> entry) {

        if ( writeDelayMs > 0 ) {
            sleepMs(writeDelayMs);
        }

        writtenKeys.put(entry.getKey(), entry.getValue());
        writeCount.incrementAndGet();
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends K, ? extends V>> entries) {

        if ( writeAllDelayMs > 0 ) {
            sleepMs(writeAllDelayMs);
        }


        Iterator<Cache.Entry<? extends K, ? extends V>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            write(iterator.next());
        }
    }

    @Override
    public void delete(Object key) {

        if ( deleteDelayMs > 0 ) {
            sleepMs(deleteDelayMs);
        }

        V value = writtenKeys.remove((K)key);
        if (value != null) {
            deletedEntries.put((K) key, value);
        }
        deleteCount.incrementAndGet();
    }

    @Override
    public void deleteAll(Collection<?> entries) {

        if ( deleteAllDelayMs > 0 ) {
            sleepMs(deleteAllDelayMs);
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