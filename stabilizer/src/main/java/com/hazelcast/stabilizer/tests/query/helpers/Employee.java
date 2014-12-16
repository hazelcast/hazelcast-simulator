package com.hazelcast.stabilizer.tests.query.helpers;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Random;

public class Employee implements DataSerializable{

    public static final int MAX_AGE = 75;
    public static final double MAX_SALARY = 1000.0;

    public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
    public static Random random = new Random();

    private int id;
    private String name;
    private int age;
    private boolean active;
    private double salary;

    public Employee(String name, int age, boolean live, double salary) {
        this.name = name;
        this.age = age;
        this.active = live;
        this.salary = salary;
    }

    public Employee(int id) {
        this.id = id;
        randomizeProperties();
    }

    public Employee() {
    }

    public void randomizeProperties() {
        name = names[random.nextInt(names.length)];
        age = random.nextInt(MAX_AGE);
        active = random.nextBoolean();
        salary = random.nextDouble() * MAX_SALARY;
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

    public double getSalary() {
        return salary;
    }

    public boolean isActive() {
        return active;
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

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", active=" + active +
                ", salary=" + salary +
                '}';
    }
}
