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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.map.helpers.Employee;
import com.hazelcast.simulator.tests.map.helpers.PredicateOperationCounter;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

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
 *
 * This test also concurrently updates and modifies the employee objects in the map while the predicate queries are executing. The
 * test may also destroy the map while while predicate are executing. We verify the result of every query to ensure that the
 * objects returned fit the requirements of the query.
 */
public class MapPredicateTest {

    private enum Operation {
        PREDICATE_BUILDER,
        SQL_STRING,
        PAGING_PREDICATE,
        UPDATE_EMPLOYEE,
        DESTROY_MAP
    }

    private static final ILogger LOGGER = Logger.getLogger(MapPredicateTest.class);

    public String basename = MapPredicateTest.class.getSimpleName();
    public int keyCount = 100;
    public int pageSize = 5;

    public double predicateBuilderProb = 0.2;
    public double sqlStringProb = 0.2;
    public double pagePredicateProb = 0.2;
    public double updateEmployeeProb = 0.3;
    public double destroyProb = 0.1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Employee> map;
    private IList<PredicateOperationCounter> operationCounterList;

    private String baseAssertMessage;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        operationCounterList = targetInstance.getList(basename + "OperationCounter");

        baseAssertMessage = format("%s: %%s not matching %%s", basename);

        operationSelectorBuilder.addOperation(Operation.PREDICATE_BUILDER, predicateBuilderProb)
                .addOperation(Operation.SQL_STRING, sqlStringProb)
                .addOperation(Operation.PAGING_PREDICATE, pagePredicateProb)
                .addOperation(Operation.UPDATE_EMPLOYEE, updateEmployeeProb)
                .addOperation(Operation.DESTROY_MAP, destroyProb);
    }

    @Warmup(global = true)
    public void globalWarmup() {
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

    @Verify(global = true)
    public void globalVerify() {
        PredicateOperationCounter total = new PredicateOperationCounter();
        for (PredicateOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        LOGGER.info(format("Operation counters from %s: %s", basename, total));
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private final PredicateOperationCounter operationCounter = new PredicateOperationCounter();

        private long lastUpdateMs = System.currentTimeMillis();
        private long iterationsLastMinute = 0;
        private long maxLastMinute = Long.MIN_VALUE;
        private long minLastMinute = Long.MAX_VALUE;
        private long spendTimeMs = 0;

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) {
            long startMs = System.currentTimeMillis();

            switch (operation) {
                case PREDICATE_BUILDER:
                    predicateBuilder();
                    break;
                case SQL_STRING:
                    sqlString();
                    break;
                case PAGING_PREDICATE:
                    pagingPredicate();
                    break;
                case UPDATE_EMPLOYEE:
                    updateEmployee();
                    break;
                case DESTROY_MAP:
                    destroyMap();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            long nowMs = System.currentTimeMillis();
            long durationMs = nowMs - startMs;
            maxLastMinute = Math.max(durationMs, maxLastMinute);
            minLastMinute = Math.min(durationMs, minLastMinute);
            iterationsLastMinute++;
            spendTimeMs += durationMs;

            if (lastUpdateMs + SECONDS.toMillis(60) < nowMs) {
                double avg = spendTimeMs / (double) iterationsLastMinute;
                double perf = (iterationsLastMinute * 1000d) / (double) spendTimeMs;

                LOGGER.info(format("last minute: iterations=%d, min=%d ms, max=%d ms, avg=%.2f ms, perf=%.2f predicates/second",
                        iterationsLastMinute, minLastMinute, maxLastMinute, avg, perf));

                maxLastMinute = Long.MIN_VALUE;
                minLastMinute = Long.MAX_VALUE;
                iterationsLastMinute = 0;
                lastUpdateMs = nowMs;
            }
        }

        @Override
        protected void afterRun() {
            operationCounterList.add(operationCounter);
        }

        private void predicateBuilder() {
            int age = randomInt(Employee.MAX_AGE);
            String name = Employee.getRandomName();

            // TODO: Still broken because it relies on reflection which is dog slow, so we need an explicit AgeNamePredicate
            EntryObject entryObject = new PredicateBuilder().getEntryObject();
            Predicate agePredicate = entryObject.get("age").lessThan(age);
            Predicate ageNamePredicate = entryObject.get("name").equal(name).and(agePredicate);

            Collection<Employee> employees = map.values(ageNamePredicate);
            for (Employee emp : employees) {
                String assertMessage = format(baseAssertMessage, emp, ageNamePredicate);
                assertTrue(assertMessage, emp.getAge() < age);
                assertTrue(assertMessage, emp.getName().equals(name));
            }
            operationCounter.predicateBuilderCount++;
        }

        private void sqlString() {
            boolean active = getRandom().nextBoolean();
            int age = randomInt(Employee.MAX_AGE);

            SqlPredicate predicate = new SqlPredicate("active=" + active + " AND age >" + age);
            Collection<Employee> employees = map.values(predicate);

            for (Employee emp : employees) {
                String assertMessage = format(baseAssertMessage, emp, predicate);
                assertTrue(assertMessage, emp.isActive() == active);
                assertTrue(assertMessage, emp.getAge() > age);
            }
            operationCounter.sqlStringCount++;
        }

        private void pagingPredicate() {
            double maxSalary = getRandom().nextDouble() * Employee.MAX_SALARY;
            Predicate predicate = Predicates.lessThan("salary", maxSalary);
            SalaryComparator salaryComparator = new SalaryComparator();
            PagingPredicate pagingPredicate = new PagingPredicate(predicate, salaryComparator, pageSize);

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

            operationCounter.pagePredicateCount++;
        }

        private List<Employee> fillListWithQueryResultSet(Iterable<Employee> iterable) {
            List<Employee> list = new ArrayList<Employee>();
            for (Employee employee : iterable) {
                list.add(employee);
            }
            return list;
        }

        private void updateEmployee() {
            Integer key = randomInt(keyCount);
            Employee employee = map.get(key);
            if (employee != null) {
                employee.randomizeProperties();
                map.put(key, employee);
                operationCounter.updateEmployeeCount++;
            }
        }

        private void destroyMap() {
            map.destroy();
            initMap();
            operationCounter.destroyCount++;
        }
    }

    private static class SalaryComparator implements Comparator<Map.Entry>, Serializable {

        @Override
        public int compare(Map.Entry o1, Map.Entry o2) {
            double employee1Salary = ((Employee) (o1.getValue())).getSalary();
            double employee2Salary = ((Employee) (o2.getValue())).getSalary();

            // ascending order
            if (employee1Salary < employee2Salary) {
                return -1;
            }
            if (employee1Salary > employee2Salary) {
                return 1;
            }
            return 0;
        }
    }
}
