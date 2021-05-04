package com.hazelcast.simulator.tests.platform.nexmark.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;

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
        public Person read(ObjectDataInput in) throws IOException {
            long id = in.readLong();
            long timestamp = in.readLong();
            String name = in.readUTF();
            String state = in.readUTF();
            return new Person(id, timestamp, name, state);
        }
    }
}
