package com.hazelcast.simulator.tests.map.domain;

public interface DomainObject {
    public String getKey();

    public void setKey(String key);

    public String getStringVal();

    public void setStringVal(String stringVal);

    public double getDoubleVal();

    public void setDoubleVal(double doubleVal);

    public long getLongVal();

    public void setLongVal(long longVal);

    public int getIntVal();

    public void setIntVal(int intVal);
}
