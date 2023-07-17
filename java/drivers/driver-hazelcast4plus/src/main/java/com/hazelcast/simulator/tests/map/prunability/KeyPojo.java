package com.hazelcast.simulator.tests.map.prunability;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.io.Serializable;

public class KeyPojo implements Serializable, DataSerializable {
    int a;
    String b;
    long c;

    public KeyPojo() {
    }

    public KeyPojo(int a, String b, long c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public int getA() {
        return a;
    }

    public String getB() {
        return b;
    }

    public long getC() {
        return c;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(a);
        out.writeString(b);
        out.writeLong(c);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        a = in.readInt();
        b = in.readString();
        c = in.readLong();
    }
}