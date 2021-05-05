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
package com.hazelcast.simulator.tests.map.domain;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class DataSerializableDomainObject extends AbstractDomainObject implements DataSerializable {

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(key);
        out.writeUTF(stringVal);
        out.writeDouble(doubleVal);
        out.writeLong(longVal);
        out.writeInt(intVal);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readUTF();
        stringVal = in.readUTF();
        doubleVal = in.readDouble();
        longVal = in.readLong();
        intVal = in.readInt();
    }
}
