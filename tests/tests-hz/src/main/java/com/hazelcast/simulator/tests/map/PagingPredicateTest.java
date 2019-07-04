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

import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.helpers.Employee;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

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
public final class PagingPredicateTest extends HazelcastTest {

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

    @TimeStep
    public void timestep(ThreadState threadState) {
        createNewPredicateIfNeeded(threadState);
        if (sequentialWorker) {
            evaluatePredicate(threadState);
            threadState.predicate.nextPage();
        } else {
            goToRandomPage(threadState);
            evaluatePredicate(threadState);
        }
    }

    private void evaluatePredicate(ThreadState threadState) {
        map.entrySet(threadState.predicate);
        threadState.predicateReusedCount++;
    }

    private void createNewPredicateIfNeeded(ThreadState threadState) {
        if (threadState.predicateReusedCount == maxPredicateReuseCount) {
            threadState.predicate = createNewPredicate();
            threadState.predicateReusedCount = 0;
        }
    }

    private void goToRandomPage(ThreadState threadState) {
        int pageNumber = threadState.randomInt(maxPage);
        threadState.predicate.setPage(pageNumber);
    }

    private PagingPredicate createNewPredicate() {
        return new PagingPredicateImpl(innerPredicate, pageSize);
    }

    public final class ThreadState extends BaseThreadState {
        protected PagingPredicate predicate = createNewPredicate();
        private int predicateReusedCount;
    }
}
