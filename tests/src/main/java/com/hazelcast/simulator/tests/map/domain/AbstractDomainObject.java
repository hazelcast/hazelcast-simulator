package com.hazelcast.simulator.tests.map.domain;

abstract class AbstractDomainObject implements DomainObject {

    protected String key;
    protected String stringVal;
    protected double doubleVal;
    protected long longVal;
    protected int intVal;

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

        DomainObject that = (DomainObject) o;
        if (key != null ? !key.equals(that.getKey()) : that.getKey() != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return (key != null ? key.hashCode() : 0);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '{'
                + "key='" + key + '\''
                + ", stringVal='" + stringVal + '\''
                + ", doubleVal=" + doubleVal
                + ", longVal=" + longVal
                + ", intVal=" + intVal
                + '}';
    }
}
