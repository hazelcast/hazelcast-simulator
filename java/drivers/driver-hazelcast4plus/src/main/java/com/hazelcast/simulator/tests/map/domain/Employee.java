package com.hazelcast.simulator.tests.map.domain;

import java.io.Serializable;

public class Employee implements Serializable {
    private int id;
    private String name;
    private int age;

    public Employee(int id, String name, int age) {
        this.name = name;
        this.id = id;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }
}