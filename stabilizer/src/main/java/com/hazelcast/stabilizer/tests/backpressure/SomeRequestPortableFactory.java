package com.hazelcast.stabilizer.tests.backpressure;

import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableHook;

import java.util.Collection;

public class SomeRequestPortableFactory implements PortableFactory {

    public static final int FACTORY_ID = 10000000;

    @Override
    public Portable create(int i) {
        return new SomeRequest();
    }
}
