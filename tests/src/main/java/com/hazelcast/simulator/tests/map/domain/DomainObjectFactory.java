package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.simulator.tests.map.SerializationStrategyTest.Strategy;

public final class DomainObjectFactory {

    private final Strategy strategy;

    private DomainObjectFactory(Strategy strategy) {
        this.strategy = strategy;
    }

    public static DomainObjectFactory newFactory(Strategy strategy) {
        return new DomainObjectFactory(strategy);
    }

    public DomainObject newInstance() {
        switch (strategy) {
            case PORTABLE:
                return new PortableDomainObject();
            case SERIALIZABLE:
                return new SerializableDomainObject();
            case DATA_SERIALIZABLE:
                return new DataSerializableDomainObject();
            case IDENTIFIED_DATA_SERIALIZABLE:
                return new IdentifiedDataSerializableDomainObject();
            default:
                throw new IllegalStateException("Unknown strategy: " + strategy);
        }
    }
}
