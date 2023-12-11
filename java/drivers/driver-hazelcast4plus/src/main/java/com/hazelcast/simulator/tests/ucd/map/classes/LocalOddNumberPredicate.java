package com.hazelcast.simulator.tests.ucd.map.classes;

import com.hazelcast.query.Predicate;
import java.util.Map;

public class LocalOddNumberPredicate implements Predicate<Integer, Integer> {
    private static final long serialVersionUID = 1L;

    public LocalOddNumberPredicate() {
    }

    @Override
    public boolean apply(Map.Entry<Integer, Integer> mapEntry) {
        return mapEntry.getValue() % 2 != 0;
    }
}
