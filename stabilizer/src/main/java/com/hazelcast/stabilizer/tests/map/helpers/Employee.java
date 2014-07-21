package com.hazelcast.stabilizer.tests.map.helpers;

import java.io.Serializable;
import java.util.Random;

public class Employee implements Serializable {

    public static String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
    public static Random random = new Random();

    private int id;
    private String name;
    private int age;
    private boolean active;
    private double salary;

    public Employee(String name, int age, boolean live, double price) {
        this.name = name;
        this.age = age;
        this.active = live;
        this.salary = price;
    }

    public Employee(int id) {
        this.id = id;
        name = names[random.nextInt(names.length)];
        age = random.nextInt(100);
        active = random.nextBoolean();
        salary = random.nextDouble() * 1000.0;
    }

    public Employee() {
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