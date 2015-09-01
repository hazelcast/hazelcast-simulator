package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class IdentifiedDataSerializableDomainObject implements DomainObject, IdentifiedDataSerializable {
    public static final int CLASS_ID = 1;
    public static final int FACTORY_ID = IdentifiedDataSerializableObjectFactory.FACTORY_ID;

    private String key;
    private String stringVal;
    private double doubleVal;
    private long longVal;
    private int intVal;


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

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public String getStringVal() {
        return stringVal;
    }

    @Override
    public void setStringVal(String stringVal) {
        this.stringVal = stringVal;
    }

    @Override
    public double getDoubleVal() {
        return doubleVal;
    }

    @Override
    public void setDoubleVal(double doubleVal) {
        this.doubleVal = doubleVal;
    }

    @Override
    public long getLongVal() {
        return longVal;
    }

    @Override
    public void setLongVal(long longVal) {
        this.longVal = longVal;
    }

    @Override
    public int getIntVal() {
        return intVal;
    }

    @Override
    public void setIntVal(int intVal) {
        this.intVal = intVal;
    }

    @Override
    public int getFactoryId() {
        return FACTORY_ID;
    }

    @Override
    public int getId() {
        return CLASS_ID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IdentifiedDataSerializableDomainObject that = (IdentifiedDataSerializableDomainObject) o;
        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return (key != null ? key.hashCode() : 0);
    }
}
