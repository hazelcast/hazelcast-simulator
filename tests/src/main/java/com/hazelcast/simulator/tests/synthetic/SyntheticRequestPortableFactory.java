package com.hazelcast.simulator.tests.synthetic;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;

public class SyntheticRequestPortableFactory implements PortableFactory {

    public static final int FACTORY_ID = 10000000;

    @Override
    public Portable create(int i) {
        return new SyntheticRequest();
    }
}
