package com.hazelcast.simulator.tests.map.prunability;

import com.hazelcast.partition.PartitionAware;
import com.hazelcast.partition.strategy.AttributePartitioningStrategy;

import java.io.Serializable;
import java.util.Objects;

public class PartitionAwareKeyPojo implements Serializable ,PartitionAware<Integer> {
    int a;
    String b;
    int x;

    public PartitionAwareKeyPojo() {
    }

    public PartitionAwareKeyPojo(int a, String b, int x) {
        this.a = a;
        this.b = b;
        this.x = x;
    }

    public int getA() {
        return a;
    }

    public String getB() {
        return b;
    }

    public long getX() {
        return x;
    }

    /**
     * We're using {@link PartitionAware} as PredicateAPI equivalent of  SQL {@link AttributePartitioningStrategy}.
     */
    @Override
    public Integer getPartitionKey() {
        return Objects.hash(a, x);
    }
}