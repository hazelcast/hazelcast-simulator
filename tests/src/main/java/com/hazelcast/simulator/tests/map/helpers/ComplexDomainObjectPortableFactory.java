package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class ComplexDomainObjectPortableFactory implements PortableFactory {

    @Override
    public Portable create(int classId) {
        switch (classId) {
            case ComplexDomainObject.PORTABLE_CLASS_ID:
                return new ComplexDomainObject();
            default:
                return null;
        }
    }
}
