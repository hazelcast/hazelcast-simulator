package com.hazelcast.simulator.tests.map.helpers;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Random;

@SuppressWarnings("unused")
public class Employee implements Serializable, Comparable<Employee> {

    public static final int MAX_AGE = 75;
    public static final double MAX_SALARY = 1000.0;

    private static final String[] NAMES = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
    private static final Random RANDOM = new Random();

    private int id;
    private String name;
    private int age;
    private double salary;
    private boolean active;

    public Employee() {
    }

    public Employee(int id) {
        this.id = id;
        randomizeProperties();
    }

    public Employee(String name, int age, boolean live, double salary) {
        this.name = name;
        this.age = age;
        this.salary = salary;
        this.active = live;
    }

    public static String getRandomName() {
        return Employee.NAMES[RANDOM.nextInt(Employee.NAMES.length)];
    }

    public final void randomizeProperties() {
        name = NAMES[RANDOM.nextInt(NAMES.length)];
        age = RANDOM.nextInt(MAX_AGE);
        salary = RANDOM.nextDouble() * MAX_SALARY;
        active = RANDOM.nextBoolean();
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

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Employee employee = (Employee) o;
        if (id != employee.id) {
            return false;
        }
        if (age != employee.age) {
            return false;
        }
        if (active != employee.active) {
            return false;
        }
        if (Double.compare(employee.salary, salary) != 0) {
            return false;
        }
        if (name != null ? !name.equals(employee.name) : employee.name != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + age;
        result = 31 * result + (active ? 1 : 0);
        temp = Double.doubleToLongBits(salary);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
