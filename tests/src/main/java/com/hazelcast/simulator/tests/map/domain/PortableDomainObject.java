package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Objects;

public class PortableDomainObject implements Portable, DomainObject {
    public static final int CLASS_ID = 1;
    public static final int FACTORY_ID = PortableObjectFactory.FACTORY_ID;

    private String key;
    private String stringVal;
    private double doubleVal;
    private long longVal;
    private int intVal;

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
    public int getClassId() {
        return CLASS_ID;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PortableDomainObject data = (PortableDomainObject) o;
        return Objects.equals(key, data.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "HzData {"
                + "intVal=" + intVal
                + ", longVal=" + longVal
                + ", doubleVal=" + doubleVal
                + ", stringVal='" + stringVal + '\''
                + ", key='" + key + '\''
                + '}';
    }

}
