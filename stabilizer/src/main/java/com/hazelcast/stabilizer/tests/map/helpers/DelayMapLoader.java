package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.core.MapLoader;
import com.hazelcast.stabilizer.tests.utils.KeyLocality;

import java.util.*;

public class DelayMapLoader implements MapLoader {

    public int keyCount = 3500;
    public int keyLength = 10;
    final int size = 3500;

    public DelayMapLoader() {
    }

    @Override
    public Object load(Object key) {
        return null;
    }

    @Override
    public Map loadAll(Collection keys) {
        Map result = new HashMap();
        for (Object key : keys) {
            try {
                Thread.sleep(150);
                result.put(key, new Employee());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public Set loadAllKeys() {
        Set keys = new HashSet();
        for (int i = 0; i < size; i++) {
            keys.add(i);
        }
        return keys;
    }

}

