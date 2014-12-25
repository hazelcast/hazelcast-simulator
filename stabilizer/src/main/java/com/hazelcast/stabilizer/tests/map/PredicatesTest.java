package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
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
import com.hazelcast.stabilizer.tests.TestRunner;
import com.hazelcast.stabilizer.tests.annotations.*;
import com.hazelcast.stabilizer.tests.map.helpers.StringUtils;
import com.hazelcast.stabilizer.tests.utils.TestUtils;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.Metronome;
import com.hazelcast.stabilizer.worker.OperationSelector;
import com.hazelcast.stabilizer.worker.SimpleMetronome;
import sun.security.pkcs11.wrapper.PKCS11RuntimeException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class PredicatesTest {

    private static enum PredicateType {
        NAME,
        AGE,
        ACTIVE,
        SALARY,
    }

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
    public SerializationStrategy serializationStrategy = SerializationStrategy.IDENTIFIED_DATA_SERIALIZABLE;
    public boolean customPredicate = false;
    public String sqlQuery = "age = 30 AND active = true";
    public double nameProb= 0.0;
    public double ageProb = 1.0;
    public double activeProb = 0.0;
    public double salaryProb = 0.0;

    public String basename = this.getClass().getName();
    public IntervalProbe search;
    public int intervalMs = 0;

    private IMap<String, EmployeeImpl> map;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private OperationSelector<PredicateType> selector = new OperationSelector<PredicateType>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.targetInstance = testContext.getTargetInstance();
        this.map = targetInstance.getMap(basename + "-" + testContext.getTestId());

        selector.addOperation(PredicateType.NAME,nameProb)
                .addOperation(PredicateType.AGE, ageProb)
                .addOperation(PredicateType.ACTIVE, activeProb)
                .addOperation(PredicateType.SALARY, salaryProb);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(TestUtils.getOperationCountInformation(targetInstance));
    }

    //configured true to prevent map size changes from number of clients and members' effect.
    @Warmup(global = true)
    public void warmup() throws InterruptedException {
        Random random = new Random();
        for (int k = 0; k < keyCount; k++) {
            int id = random.nextInt();
            String key = StringUtils.generateString(keyLength);
            EmployeeImpl value = createEmployee(id);
            map.put(key, value);
        }
        log.info("Map size is:" + map.size());
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
            PredicateType type = selector.select();
            AgePredicate agePredicate = new AgePredicate(30);
            NamePredicate namePredicate = new NamePredicate("aaa");
            ActivePredicate activePredicate = new ActivePredicate(true);
            SalaryPredicate salaryPredicate = new SalaryPredicate(700.0);
            while (!testContext.isStopped()) {
                metronome.waitForNext();
                search.started();
                if (customPredicate) {
                    switch (type){
                        case AGE:
                            map.values(agePredicate);
                            break;
                        case NAME:
                            map.values(namePredicate);
                            break;
                        case ACTIVE:
                            map.values(activePredicate);
                            break;
                        case SALARY:
                            map.values(salaryPredicate);
                            break;
                    }
                } else {
                    map.values(sqlPredicate);
                }
                search.done();

                iteration++;
                if (iteration % logFrequency == 0) {
                    log.info(Thread.currentThread().getName() + " At iteration: " + iteration);
                }

                if (iteration % performanceUpdateFrequency == 0) {
                    operations.addAndGet(performanceUpdateFrequency);
                }

            }
            operations.addAndGet(iteration % performanceUpdateFrequency);
        }
    }

    //Serialization Types
    public EmployeeImpl createEmployee(int id) {
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

    //Custom Predicates
    private static class NamePredicate implements Predicate<String, EmployeeImpl> {
        private final String name;

        private NamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Map.Entry<String, EmployeeImpl> entry) {
            return entry.getValue().getName().equals(name);
        }
    }

    private static class AgePredicate implements Predicate<String, EmployeeImpl> {
        private final int age;

        public AgePredicate(int age) {
            this.age = age;
        }

        @Override
        public boolean apply(IMap.Entry<String, EmployeeImpl> entry) {
            return entry.getValue().getAge() == age;
        }
    }

    private static class SalaryPredicate implements Predicate<String, EmployeeImpl> {
        private final double salary;

        private SalaryPredicate(double salary) {
            this.salary = salary;
        }

        @Override
        public boolean apply(IMap.Entry<String, EmployeeImpl> entry) {
            return entry.getValue().getSalary() == salary;
        }
    }

    private static class ActivePredicate implements Predicate<String, EmployeeImpl> {
        private final boolean active;

        private ActivePredicate(boolean active) {
            this.active = active;
        }

        @Override
        public boolean apply(IMap.Entry<String, EmployeeImpl> entry) {
            return entry.getValue().isActive() == active;
        }
    }

    public abstract static class EmployeeImpl {

        public static final int MAX_AGE = 75;
        public static final int MIN_AGE = 18;
        public static final double MAX_SALARY = 1000.0;
        public static final double MIN_SALARY = 1.0;

        public static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        public static Random random = new Random();

        protected int id;
        protected String name;
        protected int age;
        protected boolean active;
        protected double salary;

        public EmployeeImpl() {
        }

        public int getEmployeeId() {
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

        public void randomizeProperties() {
            name = names[random.nextInt(names.length)];
            age = MIN_AGE + random.nextInt(MAX_AGE);
            active = random.nextBoolean();
            salary = MIN_SALARY + random.nextDouble() * MAX_SALARY;
        }

        @Override
        public String toString() {
            return "EmployeeImpl{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    ", active=" + active +
                    ", salary=" + salary +
                    '}';
        }
    }

    public static class DataSerializableEmployee extends EmployeeImpl implements DataSerializable {

        public DataSerializableEmployee(int id) {
            this.id = id;
            randomizeProperties();
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
    }

    public static class EmployeeDataSerializableFactory implements DataSerializableFactory{

        public static final int FACTORY_ID = 3000;

        @Override
        public IdentifiedDataSerializable create(int i) {
                return new IdentifiedDataSerializableEmployee();
        }
    }


    public static class IdentifiedDataSerializableEmployee extends EmployeeImpl implements IdentifiedDataSerializable{

        public IdentifiedDataSerializableEmployee() {
        }

        public IdentifiedDataSerializableEmployee(int id) {
            this.id = id;
            randomizeProperties();
        }

        @Override
        public int getFactoryId() {
            return EmployeeDataSerializableFactory.FACTORY_ID;
        }

        @Override
        public int getId() {
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
    }

    public static class SerializableEmployee extends EmployeeImpl implements Serializable {

        public SerializableEmployee(int id) {
            this.id = id;
            randomizeProperties();
        }

    }

    public static class PortableEmployeeFactory implements PortableFactory {

        public static final int FACTORY_ID = 300000;

        @Override
        public PortableEmployee create(int i){
            return new PortableEmployee();
        }
    }


    public static class PortableEmployee extends EmployeeImpl implements Portable {

        public PortableEmployee() {
        }

        public PortableEmployee(int id) {
            this.id = id;
            randomizeProperties();
        }

        @Override
        public int getFactoryId() {
            return PortableEmployeeFactory.FACTORY_ID;
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
    }

    public static void main(String[] args) throws Throwable {
        PredicatesTest test = new PredicatesTest();
        new TestRunner<PredicatesTest>(test).run();
    }
}
