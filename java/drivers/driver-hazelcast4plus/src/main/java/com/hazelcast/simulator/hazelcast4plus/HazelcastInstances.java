package com.hazelcast.simulator.hazelcast4plus;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.drivers.Convertible;

import java.util.List;

public class HazelcastInstances implements Convertible {

    private final List<HazelcastInstance> values;

    public HazelcastInstances(List<HazelcastInstance> values) {
        this.values = List.copyOf(values);
    }

    public List<HazelcastInstance> values() {
        return values;
    }

    @Override
    public Object convertTo(Class<?> target) {
        if (HazelcastInstances.class.equals(target)) {
            return this;
        } else if (HazelcastInstance.class.equals(target)) {
            return values.isEmpty() ? null : values.get(0);
        } else {
            return null;
        }
    }
}
