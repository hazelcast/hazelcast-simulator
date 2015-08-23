package com.hazelcast.simulator.tests.map.domain;

import java.io.Serializable;
import java.util.Objects;

public class SerializableDomainObject implements DomainObject, Serializable {
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SerializableDomainObject data = (SerializableDomainObject) o;
        return Objects.equals(key, data.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
