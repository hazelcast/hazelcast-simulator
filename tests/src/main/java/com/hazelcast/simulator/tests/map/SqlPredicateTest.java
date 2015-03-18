package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.probes.probes.IntervalProbe;
import com.hazelcast.simulator.tests.helpers.StringUtils;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Performance;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.SimpleMetronome;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;

public class SqlPredicateTest {

    private static final ILogger log = Logger.getLogger(SqlPredicateTest.class);

    // properties
    public int threadCount = 40;
    public int keyLength = 10;
    // number of keys per member
    public int keyCount = 10000;
    public int logFrequency = 10000;
    public int performanceUpdateFrequency = 1;
    public String basename = "sqlpredicate";
    public String sql = "age = 30 AND active = true";
    public IntervalProbe search;
    public int intervalMs = 0;

    private IMap<String, DataSerializableEmployee> map;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private final AtomicLong operations = new AtomicLong();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        this.targetInstance = testContext.getTargetInstance();
        this.map = targetInstance.getMap(basename + "-" + testContext.getTestId());
    }

    @Teardown
    public void teardown() throws Exception {
        map.destroy();
        log.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() throws InterruptedException {
        Random random = new Random();
        for (int k = 0; k < keyCount; k++) {
            int id = random.nextInt();
            String key = StringUtils.generateString(keyLength);
            DataSerializableEmployee value = new DataSerializableEmployee(id);
            map.put(key, value);
        }
        log.info("Map size is:" + map.size());
        log.info("Map localKeySet size is: "+map.localKeySet().size());
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
            SqlPredicate sqlPredicate = new SqlPredicate(sql);

            while (!testContext.isStopped()) {
                metronome.waitForNext();
                search.started();
                map.values(sqlPredicate);
                search.done();
                
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

    public static class DataSerializableEmployee implements DataSerializable {

        public static final int MAX_AGE = 75;
        public static final double MAX_SALARY = 1000.0;

        private static final String[] names = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};
        private static final Random random = new Random();

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
}
