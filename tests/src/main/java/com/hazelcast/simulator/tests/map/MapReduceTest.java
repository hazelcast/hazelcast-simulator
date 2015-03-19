package com.hazelcast.simulator.tests.map;

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
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.map.helpers.Employee;
import com.hazelcast.simulator.tests.map.helpers.MapReduceOperationCounter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class MapReduceTest {

    private enum Operation {
        MAP_REDUCE,
        GET_MAP_ENTRY,
        MODIFY_MAP_ENTRY
    }

    private static final ILogger log = Logger.getLogger(MapReduceTest.class);

    // properties
    public String baseName = MapReduceTest.class.getSimpleName();
    public int keyCount = 1000;

    public double mapReduceProb = 0.5;
    public double getMapEntryProb = 0.25;
    public double modifyEntryProb = 0.25;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private HazelcastInstance targetInstance;
    private IMap<Integer, Employee> map;
    private IList<MapReduceOperationCounter> operationCounterList;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        targetInstance = testContext.getTargetInstance();

        map = targetInstance.getMap(baseName);
        operationCounterList = targetInstance.getList(baseName + "OperationCounter");

        operationSelectorBuilder.addOperation(Operation.MAP_REDUCE, mapReduceProb)
                                .addOperation(Operation.GET_MAP_ENTRY, getMapEntryProb)
                                .addOperation(Operation.MODIFY_MAP_ENTRY, modifyEntryProb);
    }

    @Warmup(global = true)
    public void warmup() throws InterruptedException {
        for (int id = 0; id < keyCount; id++) {
            map.put(id, new Employee(id));
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        MapReduceOperationCounter total = new MapReduceOperationCounter();
        for (MapReduceOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        log.info(baseName + ": " + total + " from " + operationCounterList.size() + " worker threads");
    }

    @RunWithWorker
    public AbstractWorker<Operation> createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {
        private final MapReduceOperationCounter operationCounter = new MapReduceOperationCounter();

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) {
            switch (operation) {
                case MAP_REDUCE:
                    mapReduce();
                    break;
                case GET_MAP_ENTRY:
                    getMapEntry();
                    break;
                case MODIFY_MAP_ENTRY:
                    modifyMapEntry();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private void mapReduce() {
            JobTracker tracker = targetInstance.getJobTracker(Thread.currentThread().getName() + baseName);
            Job<Integer, Employee> job = tracker.newJob(KeyValueSource.fromMap(map));

            ICompletableFuture<Map<Integer, Set<Employee>>> future = job
                    .mapper(new ModIdMapper(2))
                    .combiner(new RangeIdCombinerFactory(10, 30))
                    .reducer(new IdReducerFactory(10, 20, 30))
                    .submit();

            try {
                Map<Integer, Set<Employee>> result = future.get();

                for (Set<Employee> set : result.values()) {
                    for (Employee employee : set) {

                        assertTrue(employee.getId() % 2 == 0);
                        assertTrue(employee.getId() >= 10 && employee.getId() <= 30);
                        assertTrue(employee.getId() != 10);
                        assertTrue(employee.getId() != 20);
                        assertTrue(employee.getId() != 30);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            operationCounter.mapReduce++;
        }

        private void getMapEntry() {
            map.get(randomInt(keyCount));

            operationCounter.getMapEntry++;
        }

        private void modifyMapEntry() {
            Employee employee = map.get(randomInt(keyCount));
            employee.randomizeProperties();
            map.put(employee.getId(), employee);

            operationCounter.modifyMapEntry++;
        }

        @Override
        protected void afterRun() {
            operationCounterList.add(operationCounter);
        }
    }

    private static class ModIdMapper implements Mapper<Integer, Employee, Integer, Employee> {
        private int mod = 0;

        private ModIdMapper(int mod) {
            this.mod = mod;
        }

        @Override
        public void map(Integer key, Employee employee, Context<Integer, Employee> context) {
            if (employee.getId() % mod == 0) {
                context.emit(key, employee);
            }
        }
    }

    private static class RangeIdCombinerFactory implements CombinerFactory<Integer, Employee, Set<Employee>> {
        private final int min;
        private final int max;

        private RangeIdCombinerFactory(int min, int max) {
            this.min = min;
            this.max = max;
        }

        @Override
        public Combiner<Employee, Set<Employee>> newCombiner(Integer key) {
            return new EmployeeCombiner();
        }

        private class EmployeeCombiner extends Combiner<Employee, Set<Employee>> {
            private Set<Employee> passed = new HashSet<Employee>();

            @Override
            public void combine(Employee employee) {
                if (employee.getId() >= min && employee.getId() <= max) {
                    passed.add(employee);
                }
            }

            @Override
            public Set<Employee> finalizeChunk() {
                if (passed.isEmpty()) {
                    return null;
                }
                return passed;
            }

            @Override
            public void reset() {
                passed = new HashSet<Employee>();
            }
        }
    }

    private static class IdReducerFactory implements ReducerFactory<Integer, Set<Employee>, Set<Employee>> {

        private int[] removeIds = null;

        public IdReducerFactory(int... removeIds) {
            this.removeIds = removeIds;
        }

        @Override
        public Reducer<Set<Employee>, Set<Employee>> newReducer(Integer key) {
            return new EmployeeReducer();
        }

        private class EmployeeReducer extends Reducer<Set<Employee>, Set<Employee>> {

            private volatile Set<Employee> passed = new HashSet<Employee>();

            @Override
            public void reduce(Set<Employee> set) {
                for (Employee employee : set) {
                    boolean add = true;
                    for (int id : removeIds) {
                        if (employee.getId() == id) {
                            add = false;
                            break;
                        }
                    }
                    if (add) {
                        passed.add(employee);
                    }
                }
            }

            @Override
            public Set<Employee> finalizeReduce() {
                if (passed.isEmpty()) {
                    return null;
                }
                return passed;
            }
        }
    }

    public static class EmployeeCollator implements Collator<Map.Entry<Integer, Set<Employee>>, Map<Integer, Set<Employee>>> {
        @Override
        public Map<Integer, Set<Employee>> collate(Iterable<Map.Entry<Integer, Set<Employee>>> values) {
            Map<Integer, Set<Employee>> result = new HashMap<Integer, Set<Employee>>();
            for (Map.Entry<Integer, Set<Employee>> entry : values) {
                for (Employee employee : entry.getValue()) {
                    result.put(employee.getId(), entry.getValue());
                }
            }
            return result;
        }
    }
}
