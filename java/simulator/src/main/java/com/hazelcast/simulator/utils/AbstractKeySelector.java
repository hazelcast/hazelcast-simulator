package com.hazelcast.simulator.utils;

import java.io.File;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.IntToLongFunction;

import static com.hazelcast.simulator.utils.FileUtils.getUserDir;

abstract class AbstractKeySelector implements KeySelector {
    private final AtomicLongArray buckets = new AtomicLongArray(100);
    private final long keyDomain;
    private final File bucketsCsv;

    AbstractKeySelector(long keyDomain) {
        this.keyDomain = keyDomain;
        this.bucketsCsv = new File(getUserDir(), "key_bucket_dist.csv");
    }

    @Override
    public void prepareKeyDistributionFile() {
        FileUtils.appendText("test_id,bucket,hits,percentage\n", bucketsCsv);
    }

    @Override
    public long nextKey(IntToLongFunction randomFn) {
        long rndKey = nextKey0(randomFn);
        int bucketIdx = (int) ((double) rndKey / keyDomain * 100);
        buckets.incrementAndGet(bucketIdx);
        return rndKey;
    }


    protected abstract long nextKey0(IntToLongFunction randomFn);

    @Override
    public void dumpKeyDistribution(String testId) {
        long allHits = 0;
        for (int i = 0; i < buckets.length(); i++) {
            allHits += buckets.get(i);
        }

        for (int bucket = 0; bucket < buckets.length(); bucket++) {
            long bucketHits = buckets.get(bucket);
            double bucketHitPercentage = (double) bucketHits / allHits * 100;
            FileUtils.appendText(String.format("%s,%d,%d,%.4f\n", testId, bucket, bucketHits, bucketHitPercentage),
                    bucketsCsv);
        }

    }

}
