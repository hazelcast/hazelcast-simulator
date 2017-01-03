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
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IQueue;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static org.junit.Assert.assertTrue;

public class CardinalityEstimatorTest extends AbstractTest {

    // properties‚
    public long elementCount = 10000000;
    public long batchSize = 1000000;
    public float deviationLimit = 0.1f;

    private IAtomicLong elementCounter;
    private IQueue<Range> batchRangeQueue;
    private CardinalityEstimator cardinalityEstimator;
    private int tolerance;
    //private boolean isBatchAlloted;

    @Setup
    public void setup() {
        cardinalityEstimator = targetInstance.getCardinalityEstimator(name);
        batchRangeQueue = targetInstance.getQueue(name);
        tolerance = (int) (elementCount * deviationLimit);
    }

    @TimeStep
    public void loadData() {
        if (elementCounter.get() < elementCount) {
            Range batchRange = new Range(elementCounter.get(), elementCounter.get() + batchSize);
            batchRangeQueue.add(batchRange);
            elementCounter.getAndAdd(batchSize);
            logger.info("Running range : " + batchRange.toString());

            for (long i = batchRange.startIndex; i < batchRange.endIndex; i++) {
                cardinalityEstimator.add(i);
            }
        }
    }

    @Verify
    public void verify() {
        assertTrue(Math.abs(cardinalityEstimator.estimate() - addAllAvailableRanges()) < tolerance);
    }

    @Teardown
    public void teardown() {
        cardinalityEstimator.destroy();
    }

    private long addAllAvailableRanges() {
        long totalSize = 0L;
        for (Range range : batchRangeQueue) {
            totalSize += range.rangeSize;
        }
        return totalSize;
    }

    private final class Range {
        long startIndex;
        long endIndex;
        long rangeSize = endIndex - startIndex;

        Range(long startIndex, long endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public String toString() {
            return "(" + startIndex + " : " + endIndex + ")";
        }
    }
}
