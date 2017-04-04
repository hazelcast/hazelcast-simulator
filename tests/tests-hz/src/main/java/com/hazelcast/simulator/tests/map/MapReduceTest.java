/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
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
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.Employee;
import com.hazelcast.simulator.tests.map.helpers.MapReduceOperationCounter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class MapReduceTest extends AbstractTest {

    // properties
    public int keyCount = 1000;

    private IMap<Integer, Employee> map;
    private IList<MapReduceOperationCounter> operationCounterList;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        operationCounterList = targetInstance.getList(name + "OperationCounter");
    }

    @Prepare(global = true)
    public void prepare() {
        for (int id = 0; id < keyCount; id++) {
            map.put(id, new Employee(id));
        }
    }

    @TimeStep(prob = 0.5)
    public void mapReduce(ThreadState state) throws Exception {
        JobTracker tracker = targetInstance.getJobTracker(Thread.currentThread().getName() + name);
        KeyValueSource<Integer, Employee> source = KeyValueSource.fromMap(map);
        Job<Integer, Employee> job = tracker.newJob(source);

        ICompletableFuture<Map<Integer, Set<Employee>>> future = job
                .mapper(new ModIdMapper(2))
                .combiner(new RangeIdCombinerFactory(10, 30))
                .reducer(new IdReducerFactory(10, 20, 30))
                .submit();

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

        state.operationCounter.mapReduce++;
    }

    @TimeStep(prob = 0.25)
    public void getMapEntry(ThreadState state) {
        map.get(state.randomInt(keyCount));

        state.operationCounter.getMapEntry++;
    }

    @TimeStep(prob = 0.25)
    public void modifyMapEntry(ThreadState state) {
        Employee employee = map.get(state.randomInt(keyCount));
        employee.randomizeProperties();
        map.put(employee.getId(), employee);

        state.operationCounter.modifyMapEntry++;
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounterList.add(state.operationCounter);
    }

    public class ThreadState extends BaseThreadState {

        private final MapReduceOperationCounter operationCounter = new MapReduceOperationCounter();
    }

    @Verify(global = true)
    public void globalVerify() {
        MapReduceOperationCounter total = new MapReduceOperationCounter();
        for (MapReduceOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        logger.info(name + ": " + total + " from " + operationCounterList.size() + " worker threads");
    }

    private static final class ModIdMapper implements Mapper<Integer, Employee, Integer, Employee> {

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

    private static final class RangeIdCombinerFactory implements CombinerFactory<Integer, Employee, Set<Employee>> {

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

    private static final class IdReducerFactory implements ReducerFactory<Integer, Set<Employee>, Set<Employee>> {

        private int[] removeIds = null;

        private IdReducerFactory(int... removeIds) {
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

    private static final class EmployeeCollator
            implements Collator<Map.Entry<Integer, Set<Employee>>, Map<Integer, Set<Employee>>> {
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
