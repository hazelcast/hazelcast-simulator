package com.hazelcast.simulator.tests.vector;

import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.VectorValues;

import javax.annotation.Nonnull;

/**
 * Vector document with int value. Uses zero-config compact serialization.
 */
public record CompactIntegerVectorDocument(int value, VectorValues vectors) implements VectorDocument<Integer> {

    @Nonnull
    @Override
    public Integer getValue() {
        return value;
    }

    @Nonnull
    @Override
    public VectorValues getVectors() {
        return vectors;
    }

    public static CompactIntegerVectorDocument of(int value, @Nonnull VectorValues vv) {
        return new CompactIntegerVectorDocument(value, vv);
    }

    public static CompactIntegerVectorDocument of(int value, @Nonnull float[] vector) {
        return new CompactIntegerVectorDocument(value, VectorValues.of(vector));
    }
}
