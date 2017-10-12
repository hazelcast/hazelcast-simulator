/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.tests.map.helpers.Employee;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;

/**
 * Test to exercising PagingPredicate.
 * It's intended to be used for benchmarking purposes, it doesn't validate correctness of results.
 *
 * It has 2 working modes:
 * <ol>
 * <li>Sequential Mode - where workers are paging in a sequential orders. Ie. Page1, Page2, PageN, PageN+1, etc</li>
 * <li>Random Mode - where workers are selecting arbitrary pages</li>
 * </ol>
 *
 * Implementation note: There is a small code duplication in worker implementations - it could be eliminated by
 * introducing a common superclass, but I believe it would just make things more complicated.
 */
public class PagingPredicateTest extends AbstractTest {

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
    public void setUp() {
        map = targetInstance.getMap(name);
        innerPredicate = new SqlPredicate(innerPredicateQuery);
    }

    @Prepare(global = true)
    public void globalPrepare() {
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
        return sequentialWorker ? new SequentialWorker() : new ArbitraryWorker();
    }

    private class SequentialWorker extends BaseWorker {

        @Override
        protected void timeStep() throws Exception {
            createNewPredicateIfNeeded();
            evaluatePredicate();
            predicate.nextPage();
        }
    }

    private class ArbitraryWorker extends BaseWorker {

        ArbitraryWorker() {
            predicate = createNewPredicate();
        }

        @Override
        protected void timeStep() throws Exception {
            createNewPredicateIfNeeded();
            goToRandomPage();
            evaluatePredicate();
        }

        private PagingPredicate createNewPredicate() {
            return new PagingPredicate(pageSize);
        }

        private void goToRandomPage() {
            int pageNumber = randomInt(maxPage);
            predicate.setPage(pageNumber);
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
