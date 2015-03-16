package com.hazelcast.simulator.tests.map.helpers;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Random;

public class Employee implements Serializable, Comparable<Employee> {

    public static final int MAX_AGE = 75;
    public static final double MAX_SALARY = 1000.0;

    private static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
    private static final Random random = new Random();

    private int id;
    private String name;
    private int age;
    private boolean active;
    private double salary;

    @SuppressWarnings("unused")
    public Employee() {
    }

    public Employee(int id) {
        this.id = id;
        randomizeProperties();
    }

    @SuppressWarnings("unused")
    public Employee(String name, int age, boolean live, double salary) {
        this.name = name;
        this.age = age;
        this.active = live;
        this.salary = salary;
    }

    public static String getRandomName() {
        return Employee.names[random.nextInt(Employee.names.length)];
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
    public String toString() {
        return "Employee{"
                + "id=" + id
                + ", name='" + name + '\''
                + ", age=" + age
                + ", active=" + active
                + ", salary=" + salary
                + '}';
    }

    @Override
    public int compareTo(@Nonnull Employee employee) {
        return id - employee.id;
    }
}