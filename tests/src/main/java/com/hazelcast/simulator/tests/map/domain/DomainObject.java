package com.hazelcast.simulator.tests.map.domain;

@SuppressWarnings("unused")
public interface DomainObject {

    String getKey();

    void setKey(String key);

    String getStringVal();

    void setStringVal(String stringVal);

    double getDoubleVal();

    void setDoubleVal(double doubleVal);

    long getLongVal();

    void setLongVal(long longVal);

    int getIntVal();

    void setIntVal(int intVal);
}
