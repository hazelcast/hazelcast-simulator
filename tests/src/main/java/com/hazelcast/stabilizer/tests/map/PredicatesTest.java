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
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.*;
import com.hazelcast.stabilizer.tests.helpers.StringUtils;
import com.hazelcast.stabilizer.worker.metronome.Metronome;
import com.hazelcast.stabilizer.worker.metronome.SimpleMetronome;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;
import com.hazelcast.stabilizer.worker.tasks.AbstractWorkerTask;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Random;

public class PredicatesTest {

    private enum Operation {
        CUSTOM_PREDICATE,
        DEFAULT_PREDICATE
    }
    
    public static enum SerializationStrategy {
        DATA_SERIALIZABLE,
        JAVA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE,
        PORTABLE,
    }

    private final static ILogger log = Logger.getLogger(PredicatesTest.class);
    // properties
    public int threadCount = 40;
    public int keyLength = 10;
    // number of keys per member
    public int keyCount = 10000;
    public int performanceUpdateFrequency = 1;
    public String name = "aaa";
    public String sqlQuery = "name = " + name;
    public String basename = "predicatesTest";
    //default
    public SerializationStrategy serializationStrategy = SerializationStrategy.PORTABLE;
    public double defaultPredicateProb = 1.0;
    public IntervalProbe search;
    public int intervalMs = 0;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
    private IMap<String, Employee> map;
    private TestContext testContext;
    private HazelcastInstance targetInstance;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.targetInstance = testContext.getTargetInstance();
        this.map = targetInstance.getMap(basename + "-" + testContext.getTestId());
        operationSelectorBuilder.addOperation(Operation.DEFAULT_PREDICATE,defaultPredicateProb).addDefaultOperation(Operation.CUSTOM_PREDICATE);
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
    }

    //configured true to prevent map size changes from number of clients and members' effect.
    @Warmup(global = true)
    public void warmup() throws InterruptedException {
        Random random = new Random();
        for (int k = 0; k < keyCount; k++) {
            int id = random.nextInt();
            String key = StringUtils.generateString(keyLength);
            String name = names[random.nextInt(names.length)];
            Employee value = createEmployee(id,name);
            map.put(key, value);
        }
        log.info("Map size is:" + map.size());
    }

    @RunWithWorker
    public AbstractWorkerTask createWorker() {
        return new WorkerTask();
    }
    
    private class WorkerTask extends AbstractWorkerTask<Operation> {
        public WorkerTask(){
            super(operationSelectorBuilder);
        }
        @Override
        public void timeStep(Operation operation) {
            Metronome metronome = SimpleMetronome.withFixedIntervalMs(intervalMs);
            SqlPredicate sqlPredicate = new SqlPredicate(sqlQuery);
            NamePredicate namePredicate = new NamePredicate("aaa");
            metronome.waitForNext();
            search.started();
            switch (operation) {
                case DEFAULT_PREDICATE:
                    map.values(namePredicate);
                    break;
                case CUSTOM_PREDICATE:
                    map.values(sqlPredicate);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            search.done();
        }
    }

    public Employee createEmployee(int id,String name) {
        switch (serializationStrategy) {
            case DATA_SERIALIZABLE:
                return new DataSerializableEmployee(id,name);
            case JAVA_SERIALIZABLE:
                return new SerializableEmployee(id,name);
            case IDENTIFIED_DATA_SERIALIZABLE:
                return new IdentifiedDataSerializableEmployee(id,name);
            case PORTABLE:
                return new PortableEmployee(id,name);
        }
        return null;
    }

    //Custom Predicates
    private static class NamePredicate implements Predicate<String, Employee> {
        private final String name;

        private NamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Map.Entry<String, Employee> entry) {
            Employee value = entry.getValue();
            return value.getName().equals(name);
        }
    }

    public interface Employee {

        public String getName();
    }

    public static class DataSerializableEmployee implements DataSerializable,Employee {

        protected int id;
        protected String name;

        public String getName() {
            return name;
        }

        public DataSerializableEmployee() {
        }

        public DataSerializableEmployee(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public void writeData(ObjectDataOutput objectDataOutput) throws IOException {
            objectDataOutput.writeInt(id);
            objectDataOutput.writeUTF(name);
        }

        @Override
        public void readData(ObjectDataInput objectDataInput) throws IOException {
            id = objectDataInput.readInt();
            name = objectDataInput.readUTF();
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class EmployeeDataSerializableFactory implements DataSerializableFactory{

        public static final int FACTORY_ID = 30;

        @Override
        public IdentifiedDataSerializable create(int i) {
            return new IdentifiedDataSerializableEmployee();
        }
    }


    public static class IdentifiedDataSerializableEmployee implements IdentifiedDataSerializable,Employee {
        protected int id;
        protected String name;

        public String getName() {
            return name;
        }

        public IdentifiedDataSerializableEmployee() {
        }

        public IdentifiedDataSerializableEmployee(int id, String name) {
            this.id = id;
            this.name = name;
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
        }

        @Override
        public void readData(ObjectDataInput objectDataInput) throws IOException {
            id = objectDataInput.readInt();
            name = objectDataInput.readUTF();
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static class SerializableEmployee implements Serializable,Employee {

        protected int id;
        protected String name;

        public String getName() {
            return name;
        }

        public SerializableEmployee(int id,String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }

    }

    public static class PortableEmployeeFactory implements PortableFactory {

        public static final int FACTORY_ID = 300000;

        @Override
        public PortableEmployee create(int i){
            return new PortableEmployee();
        }
    }


    public static class PortableEmployee implements Portable,Employee {

        protected int id;
        protected String name;

        public String getName() {
            return name;
        }

        public PortableEmployee() {
        }

        public PortableEmployee(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public int getFactoryId() {
            return PortableEmployeeFactory.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return 1;
        }

        @Override
        public void writePortable(PortableWriter portableWriter) throws IOException {
            portableWriter.writeInt("id", id);
            portableWriter.writeUTF("name", name);
        }

        @Override
        public void readPortable(PortableReader portableReader) throws IOException {
            id = portableReader.readInt("id");
            name = portableReader.readUTF("name");
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }
    }

    public static void main(String[] args) throws Throwable {
        PredicatesTest test = new PredicatesTest();
        new TestRunner<PredicatesTest>(test).run();
    }
}