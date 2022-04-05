/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
