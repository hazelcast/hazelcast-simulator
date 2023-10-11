package com.hazelcast.simulator.utils;

import java.util.concurrent.ThreadLocalRandom;
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

    @Override
    public String toString() {
        return "RandomKeySelector{" +
                "keyDomain=" + keyDomain +
                '}';
    }

    public static void main(String[] args) {
        int keyDomain = 25769803;
        RandomKeySelector selector = new RandomKeySelector(keyDomain);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long[] buckets = new long[100];
        for (int i = 0; i < 1000_000; i++) {
            long l = selector.nextKey(x -> (long) (rnd.nextDouble() * x));
            int bucketIdx = (int) ((double) l / keyDomain * 100);
            buckets[bucketIdx]++;

//            System.out.println(bucketIdx);
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
