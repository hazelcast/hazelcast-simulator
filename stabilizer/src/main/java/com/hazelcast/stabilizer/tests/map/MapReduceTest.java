package com.hazelcast.stabilizer.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;
import com.hazelcast.stabilizer.tests.TestContext;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.Employee;
import com.hazelcast.stabilizer.tests.utils.ThreadSpawner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.hazelcast.stabilizer.tests.utils.TestUtils.warmupPartitions;
import static org.junit.Assert.assertTrue;

public class MapReduceTest {

    private final static ILogger log = Logger.getLogger(MapReduceTest.class);

    public int threadCount = 1;
    public int keyCount = 1000;

    public double mapReduceProb=0.5;
    public double getMapEntryProb=0;
    public double modifyEntryProb=0;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private String baseName = null;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        baseName = testContext.getTestId();

        warmupPartitions(log, targetInstance);
    }

    @Warmup(global = true)
    public void warmup() throws InterruptedException {
        Map map = targetInstance.getMap(baseName);
        for (int id = 0; id < keyCount; id++) {
            map.put(id, new Employee(id));
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int i = 0; i < threadCount; i++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        private Random random = new Random();
        private Counter counter = new Counter();

        public void run() {
            while (!testContext.isStopped()) {

                double chance = random.nextDouble();
                if ((chance -= mapReduceProb) < 0) {

                    IMap<Integer, Employee> map = targetInstance.getMap(baseName);
                    JobTracker tracker = targetInstance.getJobTracker(Thread.currentThread().getName() + baseName);
                    Job<Integer, Employee> job = tracker.newJob(KeyValueSource.fromMap(map));

                    ICompletableFuture< Map< Integer, Set<Employee>> > future = job
                            .mapper( new ModIdMapper(2) )
                            .combiner(new RangeIdCombinerFactory(10, 30))
                            .reducer(new IdReducerFactory(10,20,30))
                            .submit();

                    try {
                        Map<Integer, Set<Employee>> result = future.get();

                        for(Set<Employee> s : result.values() ){
                            for(Employee e : s ){

                                assertTrue(e.getId()%2 == 0);
                                assertTrue(e.getId() >= 10 && e.getId() <= 30);
                                assertTrue(e.getId() != 10);
                                assertTrue(e.getId() != 20);
                                assertTrue(e.getId() != 30);
                            }
                        }
                    } catch (Exception e) { throw new RuntimeException(e);}

                    counter.mapReduce++;
                }
                else if ((chance -= getMapEntryProb) < 0) {
                    IMap<Integer, Employee> map = targetInstance.getMap(baseName);
                    map.get(random.nextInt(keyCount));

                    counter.getMapEntry++;
                }
                else if ((chance -= modifyEntryProb) < 0) {
                    IMap<Integer, Employee> map = targetInstance.getMap(baseName);

                    Employee e = map.get(random.nextInt(keyCount));
                    e.randomizeProperties();
                    map.put(e.getId(), e);
                    counter.modifyMapEntry++;
                }
            }
            targetInstance.getList(baseName).add(counter);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        IList<Counter> results = targetInstance.getList(baseName);
        Counter total = new Counter();
        for (Counter i : results) {
            total.add(i);
        }
        log.info(baseName + ": " + total + " from " + results.size() + " worker Threads");
    }

    public static class Counter implements Serializable {
        public long mapReduce = 0;
        public long getMapEntry=0;
        public long modifyMapEntry = 0;

        public void add(Counter o) {
            mapReduce += o.mapReduce;
            getMapEntry += o.getMapEntry;
            modifyMapEntry += o.modifyMapEntry;
        }

        @Override
        public String toString() {
            return "Counter{" +
                    "mapReduce=" + mapReduce +
                    ", getMapEntry=" + getMapEntry +
                    ", modifyMapEntry=" + modifyMapEntry +
                    '}';
        }
    }

    public static class ModIdMapper implements Mapper<Integer, Employee, Integer, Employee> {
        private int mod=0;

        public ModIdMapper(int mod){
            this.mod=mod;
        }

        public void map(Integer key, Employee e, Context<Integer, Employee> context) {
            if(e.getId()%mod==0){
                context.emit(key, e);
            }
        }
    }

    public static class RangeIdCombinerFactory implements CombinerFactory<Integer, Employee, Set<Employee>> {
        private int min=0, max=0;

        public RangeIdCombinerFactory(int min, int max){
            this.min=min;
            this.max=max;
        }

        public Combiner<Employee, Set<Employee>> newCombiner(Integer key) {
            return new  EmployeeCombiner();
        }

        private class  EmployeeCombiner extends Combiner<Employee, Set<Employee> >{
            private Set<Employee> passed = new HashSet<Employee>();

            public void combine(Employee e) {
                if(e.getId() >= min && e.getId() <= max){
                    passed.add(e);
                }
            }

            public Set<Employee> finalizeChunk() {
                if(passed.isEmpty()){
                    return null;
                }
                return passed;
            }

            public void reset() {
                passed = new HashSet<Employee>();
            }
        }
    }

    public static class IdReducerFactory implements ReducerFactory<Integer, Set<Employee>, Set<Employee>> {

        private int[] removeIds=null;

        public IdReducerFactory(int... removeIds){
            this.removeIds=removeIds;
        }

        public Reducer<Set<Employee>, Set<Employee>> newReducer(Integer key) {
            return new EmployeeReducer();
        }

        private class EmployeeReducer extends Reducer<Set<Employee>, Set<Employee> >{

            private volatile Set<Employee> passed = new HashSet<Employee>();

            public void reduce(Set<Employee> set) {
                for(Employee e : set){
                    boolean add=true;
                    for(int id : removeIds){
                        if(e.getId()==id){
                            add=false;
                            break;
                        }
                    }
                    if(add){
                        passed.add(e);
                    }
                }
            }

            public Set<Employee> finalizeReduce() {
                if(passed.isEmpty()){
                    return null;
                }
                return passed;
            }
        }
    }

    public static class EmployeeCollator implements Collator<Map.Entry<Integer, Set<Employee>>, Map<Integer, Set<Employee>> > {

        public Map<Integer, Set<Employee>> collate( Iterable< Map.Entry<Integer, Set<Employee>> > values) {
            Map<Integer, Set<Employee>> result = new HashMap();
            for (Map.Entry<Integer, Set<Employee>> entry : values) {
                for (Employee e : entry.getValue()) {
                    result.put(e.getId(), entry.getValue());
                }
            }
            return result;
        }
    }
}
