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

import com.hazelcast.collection.IList;
import com.hazelcast.map.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.PredicateBuilderImpl;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.Employee;
import com.hazelcast.simulator.tests.map.helpers.PredicateOperationCounter;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

/**
 * In this test we are using different predicate methods to execute a query on a map of {@link Employee} objects.
 * <p>
 * This test also concurrently updates and modifies the employee objects in the map while the predicate queries are executing. The
 * test may also destroy the map while while predicate are executing. We verify the result of every query to ensure that the
 * objects returned fit the requirements of the query.
 */
public class MapPredicateTest extends HazelcastTest {

    public int keyCount = 100;
    public int pageSize = 5;

    private IMap<Integer, Employee> map;
    private IList<PredicateOperationCounter> operationCounterList;

    private String baseAssertMessage;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        operationCounterList = targetInstance.getList(name + "OperationCounter");

        baseAssertMessage = format("%s: %%s not matching %%s", name);
    }

    @Prepare(global = true)
    public void globalPrepare() {
        initMap();
    }

    private void initMap() {
        Streamer<Integer, Employee> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            Employee employee = new Employee(i);
            streamer.pushEntry(employee.getId(), employee);
        }
        streamer.await();
    }

    @TimeStep(prob = 0.2)
    public void predicateBuilder(ThreadState state) {
        long startMs = System.currentTimeMillis();

        int age = state.randomInt(Employee.MAX_AGE);
        String name = Employee.getRandomName();

        // TODO: Still broken because it relies on reflection which is dog slow, so we need an explicit AgeNamePredicate
        PredicateBuilder.EntryObject entryObject = new PredicateBuilderImpl().getEntryObject();
        Predicate agePredicate = entryObject.get("age").lessThan(age);
        Predicate ageNamePredicate = entryObject.get("name").equal(name).and(agePredicate);

        Collection<Employee> employees = map.values(ageNamePredicate);
        for (Employee emp : employees) {
            String assertMessage = format(baseAssertMessage, emp, ageNamePredicate);
            assertTrue(assertMessage, emp.getAge() < age);
            assertTrue(assertMessage, emp.getName().equals(name));
        }
        state.operationCounter.predicateBuilderCount++;

        updateStats(state, startMs);
    }

    @TimeStep(prob = 0.2)
    public void sqlString(ThreadState state) {
        long startMs = System.currentTimeMillis();
        boolean active = state.randomBoolean();
        int age = state.randomInt(Employee.MAX_AGE);

        SqlPredicate predicate = new SqlPredicate("active=" + active + " AND age >" + age);
        Collection<Employee> employees = map.values(predicate);

        for (Employee emp : employees) {
            String assertMessage = format(baseAssertMessage, emp, predicate);
            assertTrue(assertMessage, emp.isActive() == active);
            assertTrue(assertMessage, emp.getAge() > age);
        }
        state.operationCounter.sqlStringCount++;
        updateStats(state, startMs);
    }

    @TimeStep(prob = 0.2)
    public void pagePredicate(ThreadState state) {
        double maxSalary = state.randomDouble() * Employee.MAX_SALARY;
        Predicate predicate = Predicates.lessThan("salary", maxSalary);
        SalaryComparator salaryComparator = new SalaryComparator();
        PagingPredicate pagingPredicate = new PagingPredicateImpl(predicate, salaryComparator, pageSize);

        Collection<Employee> employees;
        List<Employee> employeeList;
        do {
            employees = map.values(pagingPredicate);
            employeeList = fillListWithQueryResultSet(employees);
            Employee nextEmployee;
            Employee currentEmployee;
            for (int i = 0; i < employeeList.size() - 1; i++) {
                currentEmployee = employeeList.get(i);
                nextEmployee = employeeList.get(i + 1);
                // check the order & max salary
                assertTrue(format(baseAssertMessage, currentEmployee.getSalary(), predicate),
                        currentEmployee.getSalary() <= nextEmployee.getSalary() && nextEmployee.getSalary() < maxSalary);
            }
            pagingPredicate.nextPage();
        } while (!employees.isEmpty());

        state.operationCounter.pagePredicateCount++;
    }

    private List<Employee> fillListWithQueryResultSet(Iterable<Employee> iterable) {
        List<Employee> list = new ArrayList<>();
        for (Employee employee : iterable) {
            list.add(employee);
        }
        return list;
    }

    @TimeStep(prob = 0.3)
    public void updateEmployee(ThreadState state) {
        Integer key = state.randomInt(keyCount);
        Employee employee = map.get(key);
        if (employee != null) {
            employee.randomizeProperties();
            map.put(key, employee);
            state.operationCounter.updateEmployeeCount++;
        }
    }

    @TimeStep(prob = 0.1)
    public void destroy(ThreadState state) {
        map.destroy();
        initMap();
        state.operationCounter.destroyCount++;
    }

    private void updateStats(ThreadState state, long startMs) {
        long nowMs = System.currentTimeMillis();
        long durationMs = nowMs - startMs;
        state.maxLastMinute = Math.max(durationMs, state.maxLastMinute);
        state.minLastMinute = Math.min(durationMs, state.minLastMinute);
        state.iterationsLastMinute++;
        state.spendTimeMs += durationMs;

        if (state.lastUpdateMs + SECONDS.toMillis(60) < nowMs) {
            double avg = state.spendTimeMs / (double) state.iterationsLastMinute;
            double perf = (state.iterationsLastMinute * 1000d) / (double) state.spendTimeMs;

            logger.info(format("last minute: iterations=%d, min=%d ms, max=%d ms, avg=%.2f ms, perf=%.2f predicates/second",
                    state.iterationsLastMinute, state.minLastMinute, state.maxLastMinute, avg, perf));

            state.maxLastMinute = Long.MIN_VALUE;
            state.minLastMinute = Long.MAX_VALUE;
            state.iterationsLastMinute = 0;
            state.lastUpdateMs = nowMs;
        }
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        operationCounterList.add(state.operationCounter);
    }


    public class ThreadState extends BaseThreadState {

        private final PredicateOperationCounter operationCounter = new PredicateOperationCounter();

        private long lastUpdateMs = System.currentTimeMillis();
        private long iterationsLastMinute = 0;
        private long maxLastMinute = Long.MIN_VALUE;
        private long minLastMinute = Long.MAX_VALUE;
        private long spendTimeMs = 0;
    }

    private static class SalaryComparator implements Comparator<Map.Entry>, Serializable {

        @Override
        public int compare(Map.Entry o1, Map.Entry o2) {
            double employee1Salary = ((Employee) (o1.getValue())).getSalary();
            double employee2Salary = ((Employee) (o2.getValue())).getSalary();

            return Double.compare(employee1Salary, employee2Salary);
        }
    }

    @Verify
    public void globalVerify() {
        PredicateOperationCounter total = new PredicateOperationCounter();
        for (PredicateOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        logger.info(format("Operation counters from %s: %s", name, total));
    }
}
