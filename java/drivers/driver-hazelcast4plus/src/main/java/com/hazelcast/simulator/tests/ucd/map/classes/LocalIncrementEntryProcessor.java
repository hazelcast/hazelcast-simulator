package com.hazelcast.simulator.tests.ucd.map.classes;

import com.hazelcast.map.EntryProcessor;
import java.util.Map;

public class LocalIncrementEntryProcessor implements EntryProcessor<Integer, Long, Object> {

    private final long increment;

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    public LocalIncrementEntryProcessor(long increment) {
        this.increment = increment;
    }

    @Override
    public Object process(Map.Entry<Integer, Long> entry) {
        long newValue = entry.getValue() + increment;
        entry.setValue(newValue);
        return null;
    }
}