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
package com.hazelcast.simulator.hz;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * A sample IdentifiedDataSerializable object implementation.
 */
public class IdentifiedDataSerializableMultipleIntsPojo implements IdentifiedDataSerializable {
    public int numbers0;
    public int numbers1;
    public int numbers2;
    public int numbers3;
    public int numbers4;
    public int numbers5;
    public int numbers6;
    public int numbers7;
    public int numbers8;
    public int numbers9;
    public String value;

    public IdentifiedDataSerializableMultipleIntsPojo() {
    }

    public IdentifiedDataSerializableMultipleIntsPojo(int number, String value) {
        this.numbers0 = number;
        this.numbers1 = number;
        this.numbers2 = number;
        this.numbers3 = number;
        this.numbers4 = number;
        this.numbers5 = number;
        this.numbers6 = number;
        this.numbers7 = number;
        this.numbers8 = number;
        this.numbers9 = number;
        this.value = value;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return IdentifiedDataSerializableFactory.SAMPLE_MUTLIPLE_INTS_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeInt(numbers0);
        objectDataOutput.writeInt(numbers1);
        objectDataOutput.writeInt(numbers2);
        objectDataOutput.writeInt(numbers3);
        objectDataOutput.writeInt(numbers4);
        objectDataOutput.writeInt(numbers5);
        objectDataOutput.writeInt(numbers6);
        objectDataOutput.writeInt(numbers7);
        objectDataOutput.writeInt(numbers8);
        objectDataOutput.writeInt(numbers9);
        objectDataOutput.writeString(value);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        numbers0 = objectDataInput.readInt();
        numbers1 = objectDataInput.readInt();
        numbers2 = objectDataInput.readInt();
        numbers3 = objectDataInput.readInt();
        numbers4 = objectDataInput.readInt();
        numbers5 = objectDataInput.readInt();
        numbers6 = objectDataInput.readInt();
        numbers7 = objectDataInput.readInt();
        numbers8 = objectDataInput.readInt();
        numbers9 = objectDataInput.readInt();
        value = objectDataInput.readString();
    }
}
