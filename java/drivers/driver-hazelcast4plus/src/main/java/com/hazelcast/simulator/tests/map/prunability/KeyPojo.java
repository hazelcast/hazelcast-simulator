package com.hazelcast.simulator.tests.map.prunability;

import com.hazelcast.partition.PartitionAware;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;

import java.io.Serializable;
import java.util.Objects;

public class KeyPojo implements Serializable, PartitionAware<Integer> {
    int a;
    String b;
    long c;

    public KeyPojo() {
    }

    public KeyPojo(int a, String b, long c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public int getA() {
        return a;
    }

    public String getB() {
        return b;
    }

    public long getC() {
        return c;
    }

    /**
     * We're using {@link PartitionAware} as PredicateAPI equivalent of  SQL {@link AttributePartitioningStrategy}.
     */
    @Override
    public Integer getPartitionKey() {
        return Objects.hash(a, c);
    }
}