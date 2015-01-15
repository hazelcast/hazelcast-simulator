package com.hazelcast.stabilizer.tests.map;

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
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;
import com.hazelcast.stabilizer.tests.map.helpers.Employee;
import com.hazelcast.stabilizer.tests.map.helpers.OppCounterIdxTest;
import com.hazelcast.stabilizer.test.utils.ThreadSpawner;
import com.hazelcast.stabilizer.worker.OperationSelector;

import java.util.Collection;
import java.util.Random;

import static org.junit.Assert.assertTrue;


/**
 * In this test we are using different predicate methods to execute a query on a map of Employee objects.
 * this test also concurrently updates and modifies the employee objects in the map while the predicate queries are
 * executing,  the test also destroys the map while while predicate are executing.  we also verify the result of every
 * query to ensure that the objects returned fit the requirements of the query
 */
public class MapPredicateTest {

    private static enum Operation {
        PREDICATE_BUILDER,
        SQL_STRING,
        PAGING_PREDICATE,
        UPDATE_EMPLOYEE,
        DESTROY
    }

    private final static ILogger log = Logger.getLogger(MapPredicateTest.class);

    public String basename = this.getClass().getName();
    public int threadCount = 3;
    public int keyCount = 100;

    public double predicateBuilder = 0.2;
    public double sqlString = 0.2;
    public double pagePred = 0.2;
    public double updateEmployee = 0.3;
    public double destroyProb = 0.1;
    public int pageSize = 5;


    private IMap<Integer, Employee> map;
    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private OperationSelector<Operation> selector = new OperationSelector<Operation>();

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();

        selector.addOperation(Operation.PREDICATE_BUILDER, predicateBuilder)
                .addOperation(Operation.SQL_STRING, sqlString)
                .addOperation(Operation.PAGING_PREDICATE, pagePred)
                .addOperation(Operation.UPDATE_EMPLOYEE, updateEmployee)
                .addOperation(Operation.DESTROY, destroyProb);
        map = targetInstance.getMap(basename);
    }

    @Warmup(global = true)
    public void globalWarmup() {
        initMap();
    }

    private void initMap() {

        for (int i = 0; i < keyCount; i++) {
            Employee e = new Employee(i);
            map.put(e.getId(), e);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (int k = 0; k < threadCount; k++) {
            spawner.spawn(new Worker());
        }
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {

        private final Random random = new Random();
        private OppCounterIdxTest counter = new OppCounterIdxTest();

        @Override
        public void run() {
            while (!testContext.isStopped()) {
                try {
                    final IMap<Integer, Employee> map = targetInstance.getMap(basename);
                    Operation operation = selector.select();
                    switch (operation) {
                        case PREDICATE_BUILDER: {
                            final int age = random.nextInt(Employee.MAX_AGE);
                            final String name = Employee.names[random.nextInt(Employee.names.length)];

                            // TODO: This is still broken because it relies on reflection and that is dog slow.
                            // So you need to make of an explicit AgeNamePredicate.
                            EntryObject entryObject = new PredicateBuilder().getEntryObject();
                            Predicate predicate1 = entryObject.get("age").lessThan(age);
                            Predicate predicate = entryObject.get("name").equal(name).and(predicate1);

                            Collection<Employee> employees = map.values(predicate);
                            for (Employee emp : employees) {
                                assertTrue(basename + ": " + emp + " not matching " + predicate, emp.getAge() < age);
                                assertTrue(basename + ": " + emp + " not matching " + predicate, emp.getName().equals(name));
                            }
                            counter.predicateBuilderCount++;
                            break;
                        }
                        case SQL_STRING: {
                            final boolean active = random.nextBoolean();
                            final int age = random.nextInt(Employee.MAX_AGE);

                            final SqlPredicate predicate = new SqlPredicate("active=" + active + " AND age >" + age);
                            Collection<Employee> employees = map.values(predicate);

                            for (Employee emp : employees) {
                                assertTrue(basename + ": " + emp + " not matching " + predicate, emp.isActive() == active);
                                assertTrue(basename + ": " + emp + " not matching " + predicate, emp.getAge() > age);
                            }
                            counter.sqlStringCount++;
                            break;
                        }
                        case PAGING_PREDICATE: {
                            final double maxSal = random.nextDouble() * Employee.MAX_SALARY;

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

                            counter.pagePredCount++;
                            break;
                        }
                        case UPDATE_EMPLOYEE: {
                            int key = random.nextInt(keyCount);
                            Employee e = map.get(key);
                            if (e != null) {
                                e.randomizeProperties();
                                map.put(key, e);
                                counter.updateEmployeCount++;
                            }

                            break;
                        }
                        case DESTROY: {
                            map.destroy();
                            initMap();
                            counter.destroyCount++;
                            break;
                        }
                    }
                } catch (DistributedObjectDestroyedException e) {
                }
            }
            targetInstance.getList(basename + "report").add(counter);
        }
    }

    @Verify(global = true)
    public void globalVerify() throws Exception {
        IList<OppCounterIdxTest> counters = targetInstance.getList(basename + "report");

        OppCounterIdxTest total = new OppCounterIdxTest();
        for (OppCounterIdxTest c : counters) {
            total.add(c);
        }
        log.info(basename + " " + total + " from " + counters.size()+" worker threads");
    }
}