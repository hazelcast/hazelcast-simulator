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
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static org.junit.Assert.assertTrue;

/**
 * The CardinalityEstimatorTest can be used to verify the Cardinality Estimation behavior. This test asserts the
 * estimation based on the following flow
 * 1. Test will add a configurable number (i.e batchSize) of elements in Cardinality Estimator
 * 2. Addition  of elements can have duplicates key based on duplicateKeysPercentage configured
 * 3. In assertion, test will verify that the element presents in Cardinality Estimator is not deviated more that
 * tolerance configured
 */

public class CardinalityEstimatorTest extends AbstractTest {

    // properties‚
    public long batchSize = 1000000;
    public double tolerancePercentage = 0.1;
    public double duplicateKeysPercentage = 0.1;


    private IAtomicLong elementCounter;
    private IAtomicLong rangeSelectorCounter;
    private CardinalityEstimator cardinalityEstimator;

    @Setup
    public void setup() {
        cardinalityEstimator = targetInstance.getCardinalityEstimator(name);
        elementCounter = targetInstance.getAtomicLong(name);
        rangeSelectorCounter = targetInstance.getAtomicLong("rangeSelector");
        rangeSelectorCounter.set(100L);
    }

    @TimeStep
    public void TimeStep(BaseThreadState state) {
        Range batchRange = getRange(state);
        logger.info("Running range : " + batchRange.toString());
        for (long i = batchRange.startIndex; i < batchRange.endIndex; i++) {
            cardinalityEstimator.add(i);
        }
    }

    @Verify
    public void verify() {
        int tolerance = (int) (elementCounter.get() * tolerancePercentage);
        logger.info("Actual Keys added : " + elementCounter.get());
        logger.info("Acceptable Count(i.e. tolerance) : " + (elementCounter.get() - tolerance));
        logger.info("Cardinality Estimation : " + cardinalityEstimator.estimate());
        assertTrue(Math.abs(cardinalityEstimator.estimate() - elementCounter.get()) < tolerance);
    }

    private Range getRange(BaseThreadState state) {
        if (rangeSelectorCounter.getAndIncrement() % (duplicateKeysPercentage * 100) != 0) {
            return nextRange();
        } else {
            return randomRange(state);
        }
    }

    private Range nextRange() {
        long currentCounterValue = elementCounter.getAndAdd(batchSize);
        return new Range(currentCounterValue, currentCounterValue + batchSize - 1);
    }

    private Range randomRange(BaseThreadState state) {
        long lowerRange = 0;
        long upperRange = elementCounter.get() - batchSize;
        long randomValue = lowerRange + (state.random.nextLong() * (upperRange - lowerRange));
        randomValue = (randomValue <= 0) ? batchSize : randomValue;
        return new Range(randomValue - batchSize, randomValue - 1);
    }

    @Teardown
    public void teardown() {
        cardinalityEstimator.destroy();
    }

    private final class Range {
        private long startIndex;
        private long endIndex;

        Range(long startIndex, long endIndex) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        public String toString() {
            return "(" + startIndex + " : " + endIndex + ")";
        }
    }
}
