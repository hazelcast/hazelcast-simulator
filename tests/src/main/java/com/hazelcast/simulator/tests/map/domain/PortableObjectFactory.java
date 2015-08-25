package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class PortableObjectFactory implements PortableFactory {
    public static final int FACTORY_ID = 10000001;

    @Override
    public Portable create(int classId) {
        switch (classId) {
            case PortableDomainObject.CLASS_ID:
                return new PortableDomainObject();
            default:
                throw new IllegalArgumentException("Unknown class ID" + classId);
        }
    }
}
