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

package com.hazelcast.simulator.tests.platform.nexmark.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

import javax.annotation.Nonnull;
import java.io.IOException;

public class Person extends Event {
    private final String name;
    private final String state;

    public Person(long id, long timestamp, String name, String state) {
        super(id, timestamp);
        this.name = name;
        this.state = state;
    }

    public String name() {
        return name;
    }

    public String state() {
        return state;
    }

    public static class PersonSerializer implements StreamSerializer<Person> {

        @Override
        public int getTypeId() {
            return 3;
        }

        @Override
        public void write(ObjectDataOutput out, Person person) throws IOException {
            out.writeLong(person.id());
            out.writeLong(person.timestamp());
            out.writeUTF(person.name());
            out.writeUTF(person.state());
        }

        @Override
        @Nonnull
        public Person read(ObjectDataInput in) throws IOException {
            long id = in.readLong();
            long timestamp = in.readLong();
            String name = in.readUTF();
            String state = in.readUTF();
            return new Person(id, timestamp, name, state);
        }
    }
}
