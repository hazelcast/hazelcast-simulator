package com.hazelcast.simulator.utils;

import java.util.function.IntToLongFunction;

/**
 * {@link KeySelector} implementation making record accesses
 * randomized with hot/cold data set classification. With the
 * help of this implementation tests can be setup to access
 * a hot set of records with probability expressed in percentage.
 * The hot set of the records is expressed as percentage of the
 * interval.
 * <p/>
 * Within the hot and cold set classes the access aims to be
 * uniformly distributed by using the random function provided
 * to the {@link #nextKey(IntToLongFunction)}.
 */
public class HotSetKeySelector implements KeySelector {
    private final int hotSetThreshold;
    private final int hotSetAccessPercentage;
    private final int min;
    private final int max;

    /**
     * @param min                    The lower bound of the key interval
     * @param max                    The higher bound of the key interval
     * @param hotSetAccessPercentage The probability of accessing
     *                               hot set records
     * @param hotSetPercentage       The percentage of the hot set
     *                               records in the interval
     */
    public HotSetKeySelector(int min, int max, int hotSetAccessPercentage, int hotSetPercentage) {
        this.min = min;
        this.max = max;
        this.hotSetAccessPercentage = hotSetAccessPercentage;
        this.hotSetThreshold = max - (int) ((max - min) * (float) hotSetPercentage / 100);
    }

    public int getHotSetThreshold() {
        return hotSetThreshold;
    }

    @Override
    public long nextKey(IntToLongFunction randomFn) {
        long clazz = randomFn.applyAsLong(100);
        boolean hotSet = clazz >= hotSetAccessPercentage;
        if (hotSet) {
            return randomFn.applyAsLong(hotSetThreshold) + min;
        } else {
            return randomFn.applyAsLong(max + 1 - hotSetThreshold) + hotSetThreshold;
        }
    }

    @Override
    public String toString() {
        return "HotSetKeySelector{"
                + "min=" + min
                + ", max=" + max
                + ", hotSetAccessPercentage=" + hotSetAccessPercentage
                + ", hotSetThreshold=" + hotSetThreshold
                + '}';
    }
}
