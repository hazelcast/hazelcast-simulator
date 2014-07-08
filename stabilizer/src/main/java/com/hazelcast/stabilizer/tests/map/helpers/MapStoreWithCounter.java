package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.core.MapStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MapStoreWithCounter implements MapStore<Object, Object> {

    public final Map store = new ConcurrentHashMap();

    protected AtomicInteger countStore = new AtomicInteger(0);
    protected AtomicInteger countDelete = new AtomicInteger(0);
    protected List<AtomicInteger> batchOpCountList = Collections.synchronizedList(new ArrayList<AtomicInteger>());


    public MapStoreWithCounter(){

        System.out.println("===>>"+this.getClass().getName()+" INSTANCE !!");


    }


    @Override
    public void store(Object key, Object value) {

        System.out.println("store "+key+" "+value);
        countStore.incrementAndGet();
        store.put(key, value);
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        batchOpCountList.add(new AtomicInteger(map.size()));

        countStore.addAndGet(map.size());
        for (Map.Entry<Object, Object> kvp : map.entrySet()) {
            store.put(kvp.getKey(), kvp.getValue());
        }
    }

    @Override
    public void delete(Object key) {
        countDelete.incrementAndGet();
        store.remove(key);
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        countDelete.addAndGet(keys.size());
        for (Object key : keys) {
            store.remove(key);
        }
    }

    @Override
    public Object load(Object key) {
        return store.get(key);
    }

    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        Map result = new HashMap();
        for (Object key : keys) {
            final Object v = store.get(key);
            if (v != null) {
                result.put(key, v);
            }
        }
        return result;
    }

    @Override
    public Set<Object> loadAllKeys() {
        return store.keySet();
    }

    public int getStoreOpCount() {
        return countStore.intValue();
    }

    public int getDeleteOpCount() {
        return countDelete.intValue();
    }

    public List<AtomicInteger> getBatchStoreOpCount() {
        return batchOpCountList;
    }

    public int size() {
        return store.size();
    }


    public int findNumberOfBatchsEqualWriteBatchSize(int writeBatchsize) {
        int count = 0;
        final List<AtomicInteger> batchStoreOpCount = getBatchStoreOpCount();
        for (AtomicInteger atomicInteger : batchStoreOpCount) {
            final int value = atomicInteger.intValue();
            if (value == writeBatchsize) {
                count++;
            }
        }
        return count;
    }
}