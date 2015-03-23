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
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.util.Collection;

import static java.lang.String.format;
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

    private static final ILogger log = Logger.getLogger(MapPredicateTest.class);

    public String basename = this.getClass().getSimpleName();
    public int threadCount = 3;
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

    @Setup
    public void setup(TestContext testContext) throws Exception {
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
        for (int i = 0; i < keyCount; i++) {
            Employee employee = new Employee(i);
            map.put(employee.getId(), employee);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        PredicateOperationCounter total = new PredicateOperationCounter();
        for (PredicateOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        log.info(format("Operation counters from %s: %s", basename, total));
    }

    @RunWithWorker
    public AbstractWorker<Operation> createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {
        private final PredicateOperationCounter operationCounter = new PredicateOperationCounter();

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        public void timeStep(Operation operation) {
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
                assertTrue(basename + ": " + emp + " not matching " + ageNamePredicate, emp.getAge() < age);
                assertTrue(basename + ": " + emp + " not matching " + ageNamePredicate, emp.getName().equals(name));
            }
            operationCounter.predicateBuilderCount++;
        }

        private void sqlString() {
            boolean active = getRandom().nextBoolean();
            int age = randomInt(Employee.MAX_AGE);

            SqlPredicate predicate = new SqlPredicate("active=" + active + " AND age >" + age);
            Collection<Employee> employees = map.values(predicate);

            for (Employee emp : employees) {
                assertTrue(basename + ": " + emp + " not matching " + predicate, emp.isActive() == active);
                assertTrue(basename + ": " + emp + " not matching " + predicate, emp.getAge() > age);
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
}
