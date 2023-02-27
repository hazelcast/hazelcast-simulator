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
 * With 20 integers in 'numbers' and a string with length 10 in 'value' the objects
 * estimated size is  around 500 bytes.
 */
public class IdentifiedDataSerializablePojo implements IdentifiedDataSerializable {

    public Integer[] numbers;
    public String valueField;

    public IdentifiedDataSerializablePojo() {

    }

    public IdentifiedDataSerializablePojo(Integer[] numbers, String valueField) {
        this.numbers = numbers;
        this.valueField = valueField;
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getClassId() {
        return 1;
    }

    @Override
    public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
        objectDataOutput.writeObject(numbers);
        objectDataOutput.writeString(valueField);
    }

    @Override
    public void readData(ObjectDataInput objectDataInput) throws IOException {
        numbers = objectDataInput.readObject();
        valueField = objectDataInput.readString();
    }
}
