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

import java.util.Collection;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
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

    /**
     * Used to construct an SQL predicate and to compare the returned values.
     */
    private enum Comparison {
        EQUALS("="),
        LESS_THAN("<"),
        GREATER_THAN(">");

        private final String sqlOperator;

        Comparison(String sqlOperator) {
            this.sqlOperator = sqlOperator;
        }
    }

    private static final ILogger LOGGER = Logger.getLogger(MapPredicateTest.class);

    public String basename = MapPredicateTest.class.getSimpleName();
    public int threadCount = 3;
    public int keyCount = 100;
    public int pageSize = 5;

    public boolean useFixedAge = false;
    public int fixedAge = 100;
    public Comparison ageComparison = Comparison.GREATER_THAN;

    public double predicateBuilderProb = 0.2;
    public double sqlStringProb = 0.2;
    public double pagePredicateProb = 0.2;
    public double updateEmployeeProb = 0.3;
    public double destroyProb = 0.1;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<Integer, Employee> map;
    private IList<PredicateOperationCounter> operationCounterList;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        operationCounterList = targetInstance.getList(basename + "OperationCounter");

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
    public void globalVerify() throws Exception {
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
        private long spentTimeMs = 0;
        private long maxLastMinute = Long.MIN_VALUE;
        private long minLastMinute = Long.MAX_VALUE;
        private long iterationsLastMinute = 0;

        public Worker() {
            super(operationSelectorBuilder);

            LOGGER.info("Starting worker: " + this + " for " + MapPredicateTest.class.getSimpleName());
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
            spentTimeMs += durationMs;
            maxLastMinute = Math.max(durationMs, maxLastMinute);
            minLastMinute = Math.min(durationMs, minLastMinute);
            iterationsLastMinute++;

            if (lastUpdateMs + SECONDS.toMillis(60) < nowMs) {
                double avg = spentTimeMs / (double) iterationsLastMinute;
                double perf = (iterationsLastMinute * 1000d) / (double) spentTimeMs;

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
            int age = (useFixedAge ? fixedAge : randomInt(Employee.MAX_AGE));
            String name = Employee.getRandomName();

            // TODO: still broken because it relies on reflection which is dog slow, so we need an explicit AgeNamePredicate
            EntryObject entryObject = new PredicateBuilder().getEntryObject();
            Predicate agePredicate = getPredicate(entryObject, "age", age, ageComparison);
            Predicate ageNamePredicate = entryObject.get("name").equal(name).and(agePredicate);

            Collection<Employee> employees = map.values(ageNamePredicate);
            for (Employee emp : employees) {
                String assertMessage = basename + ": " + emp + " not matching " + ageNamePredicate;
                assertInteger(assertMessage, age, emp.getAge(), ageComparison);
                assertEquals(assertMessage, name, emp.getName());
            }
            operationCounter.predicateBuilderCount++;
        }

        private void sqlString() {
            boolean active = getRandom().nextBoolean();
            int age = (useFixedAge ? fixedAge : randomInt(Employee.MAX_AGE));

            SqlPredicate predicate = new SqlPredicate("active=" + active + " AND age" + ageComparison.sqlOperator + age);

            Collection<Employee> employees = map.values(predicate);
            for (Employee emp : employees) {
                String assertMessage = basename + ": " + emp + " not matching " + predicate;
                assertEquals(assertMessage, active, emp.isActive());
                assertInteger(assertMessage, age, emp.getAge(), ageComparison);
            }
            operationCounter.sqlStringCount++;
        }

        private void pagingPredicate() {
            double maxSal = getRandom().nextDouble() * Employee.MAX_SALARY;

            Predicate predicate = Predicates.lessThan("salary", maxSal);
            PagingPredicate pagingPredicate = new PagingPredicate(predicate, pageSize);

            Collection<Employee> employees;
            do {
                employees = map.values(pagingPredicate);
                for (Employee emp : employees) {
                    assertTrue(basename + ": " + emp + " not matching " + predicate, emp.getSalary() < maxSal);
                }
                pagingPredicate.nextPage();
            } while (!employees.isEmpty());

            operationCounter.pagePredicateCount++;
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

    private static Predicate getPredicate(EntryObject entryObject, String fieldName, int age, Comparison comparison) {
        switch (comparison) {
            case EQUALS:
                return entryObject.get(fieldName).equal(age);
            case LESS_THAN:
                return entryObject.get(fieldName).lessThan(age);
            case GREATER_THAN:
                return entryObject.get(fieldName).greaterThan(age);
            default:
                throw new UnsupportedOperationException("Unsupported comparison: " + comparison);
        }
    }

    private static void assertInteger(String message, int expected, int actual, Comparison comparison) {
        switch (comparison) {
            case EQUALS:
                assertEquals(message, expected, actual);
                break;
            case LESS_THAN:
                assertTrue(message, actual < expected);
                break;
            case GREATER_THAN:
                assertTrue(message, actual > expected);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported comparison: " + comparison);
        }
    }
}
