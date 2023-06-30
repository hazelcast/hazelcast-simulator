package com.hazelcast.simulator.tests.map.prunability;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class KeyPojo implements DataSerializable {
    int a;
    String b;
    long c;

    public KeyPojo(int a, String b, long c) {
        this.a = a;
        this.b = b;
        this.c = c;
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