/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map.domain;

abstract class AbstractDomainObject implements DomainObject {

    String key;
    String stringVal;
    double doubleVal;
    long longVal;
    int intVal;

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
