package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.map.helpers.Employee;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;

import static java.lang.Math.abs;

/**
 * Test to exercising PagingPredicate.
 * It's intended to be used for benchmarking purposes, it doesn't validate correctness of results.
 *
 * It has 2 working modes:
 * <ol>
 *    <li>Sequential Mode - where workers are paging in a sequential orders. Ie. Page1, Page2, PageN, PageN+1, etc</li>
 *    <li>Random Mode - where workers are selecting arbitrary pages</li>
 * </ol>
 *
 * Implementation note: There is a small code duplication in worker implementations - it could be eliminated by
 * introducing a common superclass, but I believe it would just make things more complicated.
 */
public class PagingPredicateTest {

    public String basename = PagingPredicateTest.class.getSimpleName();
    // this is rather high number, make sure you have enough heap space, e.g. with JVM option -Xmx20g
    public int keyCount = 10000000;
    public boolean useIndex;
    public int pageSize = 10000;
    public String innerPredicateQuery = "salary < 900";
    public int maxPage = (int) (((float) keyCount / pageSize) * 0.9);
    public int maxPredicateReuseCount = 100;
    public boolean sequentialWorker = true;

    private IMap<Integer, Employee> map;
    private SqlPredicate innerPredicate;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        HazelcastInstance targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        innerPredicate = new SqlPredicate(innerPredicateQuery);
    }

    @Warmup(global = true)
    public void globalWarmup() {
        if (useIndex) {
            map.addIndex("salary", true);
        }
        Streamer<Integer, Employee> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            Employee employee = new Employee(i);
            streamer.pushEntry(employee.getId(), employee);
        }
        streamer.await();
    }

    @RunWithWorker
    public IWorker createWorker() {
        return sequentialWorker ? new SequentialWorker() : new RandomWorker();
    }

    private class SequentialWorker extends BaseWorker {

        @Override
        protected void timeStep() throws Exception {
            createNewPredicateIfNeeded();
            evaluatePredicate();
            predicate.nextPage();
        }
    }

    private class RandomWorker extends BaseWorker {

        @Override
        protected void timeStep() throws Exception {
            createNewPredicateIfNeeded();
            goToRandomPage();
            evaluatePredicate();
        }

        private void goToRandomPage() {
            int newPage = abs(randomInt(maxPage));
            while (predicate.getPage() != newPage) {
                if (predicate.getPage() > newPage) {
                    predicate.previousPage();
                } else {
                    predicate.nextPage();
                }
            }
        }
    }

    private abstract class BaseWorker extends AbstractMonotonicWorker {

        protected PagingPredicate predicate = createNewPredicate();

        private int predicateReusedCount;

        protected void createNewPredicateIfNeeded() {
            if (predicateReusedCount == maxPredicateReuseCount) {
                predicate = createNewPredicate();
                predicateReusedCount = 0;
            }
        }

        protected void evaluatePredicate() {
            map.entrySet(predicate);
            predicateReusedCount++;
        }

        private PagingPredicate createNewPredicate() {
            return new PagingPredicate(innerPredicate, pageSize);
        }
    }
}
