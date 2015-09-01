package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DataSerializableDomainObject implements DomainObject, DataSerializable {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataSerializableDomainObject that = (DataSerializableDomainObject) o;
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
