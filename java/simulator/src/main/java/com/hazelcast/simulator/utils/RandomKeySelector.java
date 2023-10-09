package com.hazelcast.simulator.utils;

import java.util.function.IntToLongFunction;

/**
 * {@link KeySelector} implementation to make the IMap records
 * randomly accessed.
 */
public class RandomKeySelector implements KeySelector {

    private final int keyDomain;

    /**
     * @param keyDomain The key domain of the test.
     */
    public RandomKeySelector(int keyDomain) {
        this.keyDomain = keyDomain;
    }

    @Override
    public long nextKey(IntToLongFunction randomFn) {
        return randomFn.applyAsLong(keyDomain);
    }
}
