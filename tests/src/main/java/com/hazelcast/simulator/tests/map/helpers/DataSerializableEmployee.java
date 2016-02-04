package com.hazelcast.simulator.tests.map.helpers;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DataSerializableEmployee extends Employee implements DataSerializable {

    @SuppressWarnings("unused")
    public DataSerializableEmployee() {
    }

    public DataSerializableEmployee(int id, String name, int age, boolean active, double salary) {
        super(id, name, age, active, salary);
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(id);
        objectDataOutput.writeUTF(name);
        objectDataOutput.writeInt(age);
        objectDataOutput.writeBoolean(active);
        objectDataOutput.writeDouble(salary);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        id = objectDataInput.readInt();
        name = objectDataInput.readUTF();
        age = objectDataInput.readInt();
        active = objectDataInput.readBoolean();
        salary = objectDataInput.readDouble();
    }
}
