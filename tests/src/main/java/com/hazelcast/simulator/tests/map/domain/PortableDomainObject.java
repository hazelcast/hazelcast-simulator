package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

public class PortableDomainObject extends AbstractDomainObject implements Portable {

    public static final int CLASS_ID = 1;
    public static final int FACTORY_ID = PortableObjectFactory.FACTORY_ID;

    @Override
    public int getClassId() {
        return CLASS_ID;
    }

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public void writePortable(PortableWriter out) throws IOException {
        out.writeUTF("key", key);
        out.writeUTF("stringVal", stringVal);
        out.writeDouble("doubleVal", doubleVal);
        out.writeLong("longVal", longVal);
        out.writeInt("intVal", intVal);
    }

    @Override
    public void readPortable(PortableReader in) throws IOException {
        key = in.readUTF("key");
        stringVal = in.readUTF("stringVal");
        doubleVal = in.readDouble("doubleVal");
        longVal = in.readLong("longVal");
        intVal = in.readInt("intVal");
    }
}
