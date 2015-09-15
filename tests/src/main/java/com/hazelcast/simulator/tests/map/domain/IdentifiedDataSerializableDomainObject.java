package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class IdentifiedDataSerializableDomainObject extends AbstractDomainObject implements IdentifiedDataSerializable {

    public static final int CLASS_ID = 1;
    public static final int FACTORY_ID = IdentifiedDataSerializableObjectFactory.FACTORY_ID;

    @Override
    public int getId() {
        return CLASS_ID;
    }

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(key);
        out.writeUTF(stringVal);
        out.writeDouble(doubleVal);
        out.writeLong(longVal);
        out.writeInt(intVal);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readUTF();
        stringVal = in.readUTF();
        doubleVal = in.readDouble();
        longVal = in.readLong();
        intVal = in.readInt();
    }
}
