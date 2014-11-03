package com.hazelcast.stabilizer.probes.probes.impl;

import java.io.Serializable;

public class HistogramPart implements Serializable {
    private final int bucket;
    private final int values;

    public HistogramPart(int bucket, int values) {
        this.bucket = bucket;
        this.values = values;
    }

    public int getBucket() {
        return bucket;
    }

    public int getValues() {
        return values;
    }
}
