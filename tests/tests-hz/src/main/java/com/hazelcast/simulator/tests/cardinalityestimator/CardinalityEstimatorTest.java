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

package com.hazelcast.simulator.tests.cardinalityestimator;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.map.IMap;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;

import java.util.Map;

import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static java.lang.Math.ceil;
import static java.lang.Math.floor;
import static org.junit.Assert.assertTrue;

/**
 * The idea behind generating unique id's is the following.
 *
 * To prevent needing to do additional remote calls to e.g. an IAtomicLong; first we determine the total number of
 * timestep threads over all worker jvm's. Then each timestep thread is given a unique start offset. So
 * thread1 start on 0
 * thread2 start on 1
 * threadn starts on n.
 *
 * Based on this unique start offset and total-threadCount, each thread can generate unique id's. E.g. if there
 * are in total 10 threads, then
 * thread1: 0, 10,20,30,...
 * thread2: 1,11,21,31,...
 *
 * To determine the total number of unique items, each thread tracks how many items it has inserted and in the
 * afterRun this will be written to an IAtomicLong per cardinality estimator.
 */
public class CardinalityEstimatorTest extends HazelcastTest {

    // properties‚
    public double tolerancePercentage = 0.1;
    // the number of cardinality estimators.  We want to have many because the estimator
    // is not a partitioned data-structure like an IMap. But more like an IAtomicLong. If we would have only a single
    // estimator, all traffic would be send to just a single node.
    public int estimatorCount = 10000;
    public KeyLocality keyLocality = SHARED;
    public int threadCount;

    private CardinalityEstimator[] estimators;
    private IMap<String, Long> expectedCountMap;
    private IAtomicLong totalThreadCount;
    private IAtomicLong threadIdGenerator;

    @Setup
    public void setup() {
        this.estimators = new CardinalityEstimator[estimatorCount];
        this.expectedCountMap = targetInstance.getMap(testContext.getTestId() + "-expectedCountMap");

        String[] names = generateStringKeys(name, estimatorCount, keyLocality, targetInstance);
        for (int i = 0; i < estimatorCount; i++) {
            String estimatorName = names[i];
            estimators[i] = targetInstance.getCardinalityEstimator(estimatorName);
            expectedCountMap.set(estimatorName, 0L);
        }

        this.totalThreadCount = targetInstance.getAtomicLong(testContext.getTestId() + "-totalThreadCount");
        this.totalThreadCount.addAndGet(threadCount);

        this.threadIdGenerator = targetInstance.getAtomicLong(testContext.getTestId() + "-threadIdGenerator");
    }

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.threadId = (int) threadIdGenerator.getAndIncrement();
        state.totalThreadCount = (int) totalThreadCount.get();

        logger.info("totalThreadCount: " + state.totalThreadCount);
        logger.info("threadId: " + state.threadId);
    }

    @TimeStep(prob = 1)
    public void add(ThreadState state) {
        int estimatorIndex = state.randomEstimatorIndex();
        long item = state.nextUniqueItem(estimatorIndex);
        estimators[estimatorIndex].add(item);
    }

    @TimeStep(prob = -1)
    public void addDuplicate(ThreadState state) {
        int estimatorIndex = state.randomEstimatorIndex();
        long item = state.nextDuplicateItem(estimatorIndex);
        estimators[estimatorIndex].add(item);
    }

    // this call doesn't do any verification since at the same moment also
    // items are being added. So the main purpose of this timestep method is to verify
    // that it doesn't cause problems other than correctness. E.g. latency, exceptions etc.
    @TimeStep(prob = 0)
    public long estimate(ThreadState state) {
        int estimatorIndex = state.randomEstimatorIndex();
        return estimators[estimatorIndex].estimate();
    }

    @AfterRun
    public void afterRun(ThreadState state) {
        // for each worker-thread we store the number of items it has produced for each estimator.
        for (int k = 0; k < estimatorCount; k++) {
            CardinalityEstimator estimator = estimators[k];
            // the number of unique items produced is equal to the iteration. If items 0,10,20 are produced,
            // then iteration is 3.
            long iteration = state.iterations[k];
            expectedCountMap.executeOnKey(estimator.getName(), new IncEntryProcessor(iteration));
        }
    }

    @Verify(global = false)
    public void verify() {
        for (int k = 0; k < estimatorCount; k++) {
            CardinalityEstimator estimator = estimators[k];
            long expected = expectedCountMap.get(estimator.getName());

            double maxError = (tolerancePercentage / 100d) * expected;
            double minExpected = floor(expected - maxError);
            double maxExpected = ceil(expected + maxError);

            long actual = estimator.estimate();
            long error = expected - actual;
            double errorPercentage = (100d * error) / expected;

            logger.info(estimator.getName() + " actual=" + actual
                    + " expected=" + expected
                    + " error=" + (errorPercentage) + "%s");

            assertTrue("too few items counted, minExpected=" + minExpected
                            + ", actual=" + actual
                            + ", error=" + errorPercentage + "%",
                    actual >= minExpected);
            assertTrue("too many items counted, maxExpected=" + maxExpected
                            + ", actual=" + actual
                            + ", error=" + errorPercentage + "%",
                    actual <= maxExpected);
        }
    }

    @Teardown(global = true)
    public void teardown() {
        for (CardinalityEstimator estimator : estimators) {
            estimator.destroy();
        }

        expectedCountMap.destroy();
        totalThreadCount.destroy();
        threadIdGenerator.destroy();
    }

    public class ThreadState extends BaseThreadState {
        private final long[] iterations = new long[estimatorCount];
        private int totalThreadCount;
        private int threadId;

        private int randomEstimatorIndex() {
            return randomInt(estimatorCount);
        }

        private long nextUniqueItem(int estimatorIndex) {
            long iteration = iterations[estimatorIndex];

            iterations[estimatorIndex] = iteration + 1;

            return toItem(iteration);
        }

        private long nextDuplicateItem(int estimatorIndex) {
            long iteration = iterations[estimatorIndex];
            if (iteration == 0) {
                // no item has ever been inserted by this thread for this given estimator.
                // so lets insert a new item instead of a duplicate.
                // This will happen only once for every worker thread in case it has not yet produced a regular item
                return nextUniqueItem(estimatorIndex);
            } else {
                // lets pick a random item from the sequence we already produced
                return toItem(randomLong(iteration));
            }
        }

        private long toItem(long iteration) {
            return totalThreadCount * iteration + threadId;
        }
    }

    private static final class IncEntryProcessor implements EntryProcessor<String, Long, Object> {
        private final long delta;

        private IncEntryProcessor(long delta) {
            this.delta = delta;
        }

        @Override
        public Object process(Map.Entry<String, Long> entry) {
            entry.setValue(entry.getValue() + delta);
            return null;
        }
    }
}
