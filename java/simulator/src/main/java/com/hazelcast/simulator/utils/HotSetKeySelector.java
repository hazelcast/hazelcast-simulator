package com.hazelcast.simulator.utils;

import java.util.concurrent.ThreadLocalRandom;
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

    public static void main(String[] args) {
        int keyDomain = 1000;
        HotSetKeySelector selector = new HotSetKeySelector(0, keyDomain - 1, 95, 10);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long[] buckets = new long[100];
        for (int i = 0; i < 1000_000; i++) {
            long l = selector.nextKey(x -> (long) (rnd.nextDouble() * x));
            int bucketIdx = (int) ((double) l / keyDomain * 100);
            buckets[bucketIdx]++;
//            System.out.println(bucketIdx);
//            System.out.println(l);
        }

        long allHits = 0;
        for (int i = 0; i < buckets.length; i++) {
            allHits += buckets[i];
        }

        for (int i = 0; i < buckets.length; i++) {
            long bucketHits = buckets[i];
            double bucketHitPercentage = (double) bucketHits / allHits * 100;
            System.out.println(String.format("Bucket %2d = %.2f%%", i, bucketHitPercentage));
        }

    }
}
