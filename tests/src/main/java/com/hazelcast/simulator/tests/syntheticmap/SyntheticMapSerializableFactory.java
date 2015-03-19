package com.hazelcast.simulator.tests.syntheticmap;

import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

public class SyntheticMapSerializableFactory implements DataSerializableFactory {

    public static final int ID = 3000;

    public static final int PUT_OPERATION = 1;
    public static final int GET_OPERATION = 2;

    @Override
    public IdentifiedDataSerializable create(int typeId) {
        switch (typeId) {
            case PUT_OPERATION:
                return new PutOperation();
            case GET_OPERATION:
                return new GetOperation();
            default:
                return null;
        }
    }
}
