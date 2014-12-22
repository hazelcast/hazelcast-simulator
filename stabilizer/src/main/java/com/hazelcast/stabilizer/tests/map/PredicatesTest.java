package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.stabilizer.probes.probes.IntervalProbe;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.Metronome;
import com.hazelcast.stabilizer.worker.SimpleMetronome;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class PredicatesTest {

    private final static ILogger log = Logger.getLogger(PredicatesTest.class);
    private final AtomicLong operations = new AtomicLong();
    // properties
    public int threadCount = 40;
    public int keyLength = 10;
    // number of keys per member
    public int keyCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1;
    //default
    public SerializationStrategy serializationStrategy = SerializationStrategy.DATA_SERIALIZABLE;
    public boolean customPredicate = false;
    public String sqlQuery = "age = 30 AND active = true";

    public String basename = this.getClass().getName();
    public IntervalProbe search;
    public int intervalMs = 0;

    private IMap<String, Employee> map;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.targetInstance = testContext.getTargetInstance();
        this.map = targetInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(TestUtils.getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        Random random = new Random();
        for (int k = 0; k < keyCount; k++) {
            int id = random.nextInt();
            String key = StringUtils.generateString(keyLength);
            Employee value = createEmployee(id);
            map.put(key, value);
        }
        log.info("Map size is:" + map.size());
//        log.info("Map localKeySet size is: " + map.localKeySet().size());
//        causes problem client side
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    @Performance
    public long getOperationCount() {
        return operations.get();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            long iteration = 0;
            Metronome metronome = SimpleMetronome.withFixedIntervalMs(intervalMs);
            SqlPredicate sqlPredicate = new SqlPredicate(sqlQuery);
            Predicate predicate = new AgePredicate(56);
            while (!testContext.isStopped()) {
                metronome.waitForNext();
                if (customPredicate == true) {
                    search.started();
                    map.values(predicate);
                    search.done();
                } else {
                    search.started();
                    map.values(sqlPredicate);
                    search.done();
                }

                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }
                iteration++;
            }

            //operations.set(iteration);
        }
    }

    //Serialization Types
    public Employee createEmployee(int id) {
        switch (serializationStrategy) {
            case DATA_SERIALIZABLE:
                return new DataSerializableEmployee(id);
            case JAVA_SERIALIZABLE:
                return new SerializableEmployee(id);
            case IDENTIFIED_DATA_SERIALIZABLE:
                return new IdentifiedDataSerializableEmployee(id);
            case PORTABLE:
                return new PortableEmployee(id);
        }
        return null;
    }

    private static enum SerializationStrategy {
        DATA_SERIALIZABLE,
        JAVA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE,
        PORTABLE
    }


    public interface Employee {
        public String getName();

        public int getAge();

        public double getSalary();

        public boolean isActive();
    }

    //Custom Predicates
    //bigger less !!
    private static class NamePredicate implements Predicate<String, Employee> {
        private final String name;

        private NamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Map.Entry<String, Employee> entry) {
            return entry.getValue().getName() == name;
        }
    }

    private static class AgePredicate implements Predicate<String, Employee> {
        private final int age;

        public AgePredicate(int age) {
            this.age = age;
        }

        @Override
        public boolean apply(IMap.Entry<String, Employee> entry) {
            return entry.getValue().getAge() == age;
        }
    }

    private static class SalaryPredicate implements Predicate<String, Employee> {
        private final double salary;

        private SalaryPredicate(double salary) {
            this.salary = salary;
        }

        @Override
        public boolean apply(IMap.Entry<String, Employee> entry) {
            return entry.getValue().getSalary() == salary;
        }
    }

    private static class ActivePredicate implements Predicate<String, Employee> {
        private final boolean active;

        private ActivePredicate(boolean active) {
            this.active = active;
        }

        @Override
        public boolean apply(IMap.Entry<String, Employee> entry) {
            return entry.getValue().isActive() == active;
        }
    }

    public static class DataSerializableEmployee implements DataSerializable, Employee {

        public static final int MAX_AGE = 75;
        public static final int MIN_AGE = 18;
        public static final double MAX_SALARY = 1000.0;
        public static final double MIN_SALARY = 1.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        private int id;
        private String name;
        private int age;
        private boolean active;
        private double salary;

        public DataSerializableEmployee(String name, int age, boolean live, double salary) {
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = salary;
        }

        public DataSerializableEmployee(int id) {
            this.id = id;
            randomizeProperties();
        }

        public DataSerializableEmployee() {
        }

        public void randomizeProperties() {
            name = names[random.nextInt(names.length)];
            age = MIN_AGE + random.nextInt(MAX_AGE);
            active = random.nextBoolean();
            salary = MIN_SALARY + random.nextDouble() * MAX_SALARY;
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

    public static class IdentifiedDataSerializableEmployee implements IdentifiedDataSerializable, Employee {

        public static final int MAX_AGE = 75;
        public static final int MIN_AGE = 18;
        public static final double MAX_SALARY = 1000.0;
        public static final double MIN_SALARY = 1.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        private int id;
        private String name;
        private int age;
        private boolean active;
        private double salary;

        public IdentifiedDataSerializableEmployee(String name, int age, boolean live, double salary) {
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = salary;
        }

        public IdentifiedDataSerializableEmployee(int id) {
            this.id = id;
            randomizeProperties();
        }

        public IdentifiedDataSerializableEmployee() {
        }

        public void randomizeProperties() {
            name = names[random.nextInt(names.length)];
            age = MIN_AGE + random.nextInt(MAX_AGE);
            active = random.nextBoolean();
            salary = MIN_SALARY + random.nextDouble() * MAX_SALARY;
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
        public int getFactoryId() {
            return 0;
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

    public static class SerializableEmployee implements Serializable, Employee {

        public static final int MAX_AGE = 75;
        public static final int MIN_AGE = 18;
        public static final double MAX_SALARY = 1000.0;
        public static final double MIN_SALARY = 1.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        private int id;
        private String name;
        private int age;
        private boolean active;
        private double salary;

        public SerializableEmployee(String name, int age, boolean live, double salary) {
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = salary;
        }

        public SerializableEmployee(int id) {
            this.id = id;
            randomizeProperties();
        }

        public SerializableEmployee() {
        }

        public void randomizeProperties() {
            name = names[random.nextInt(names.length)];
            age = MIN_AGE + random.nextInt(MAX_AGE);
            active = random.nextBoolean();
            salary = MIN_SALARY + random.nextDouble() * MAX_SALARY;
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
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    ", active=" + active +
                    ", salary=" + salary +
                    '}';
        }
    }

    public static class PortableEmployee implements Portable, Employee {

        public static final int MAX_AGE = 75;
        public static final int MIN_AGE = 18;
        public static final double MAX_SALARY = 1000.0;
        public static final double MIN_SALARY = 1.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        private int id;
        private String name;
        private int age;
        private boolean active;
        private double salary;

        public PortableEmployee(String name, int age, boolean live, double salary) {
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = salary;
        }

        public PortableEmployee(int id) {
            this.id = id;
            randomizeProperties();
        }

        public PortableEmployee() {
        }

        public void randomizeProperties() {
            name = names[random.nextInt(names.length)];
            age = MIN_AGE + random.nextInt(MAX_AGE);
            active = random.nextBoolean();
            salary = MIN_SALARY + random.nextDouble() * MAX_SALARY;
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
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getClassId() {
            return 0;
        }

        @Override
        public void writePortable(PortableWriter portableWriter) throws IOException {
            portableWriter.writeInt("id", id);
            portableWriter.writeUTF("name", name);
            portableWriter.writeInt("age", age);
            portableWriter.writeBoolean("active", active);
            portableWriter.writeDouble("salary", salary);
        }

        @Override
        public void readPortable(PortableReader portableReader) throws IOException {
            id = portableReader.readInt("id");
            name = portableReader.readUTF("name");
            age = portableReader.readInt("age");
            active = portableReader.readBoolean("active");
            salary = portableReader.readDouble("salary");
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
}
