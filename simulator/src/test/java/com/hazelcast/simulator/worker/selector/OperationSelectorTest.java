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
package com.hazelcast.simulator.worker.selector;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperationSelectorTest {

    private enum Operation {
        OP1, OP2, OP3, OP4, OP5, DEFAULT
    }

    private static final int ITERATIONS = 1000000;
    private static final double TOLERANCE = 0.05;

    private OperationSelectorBuilder<Operation> builder;
    private OperationSelector<Operation> selector;

    @Before
    public void before() {
        builder = new OperationSelectorBuilder<Operation>();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperations_sumOfProbabilitiesBelowLimit() {
        builder.addOperation(Operation.OP1, 0.80)
               .addOperation(Operation.OP2, 0.19)
               .build();
    }

    @Test
    public void testAddOperations_sumOfProbabilitiesHitsLimit() {
        builder.addOperation(Operation.OP1, 0.8)
               .addOperation(Operation.OP2, 0.2)
               .build();
    }

    @Test
    public void testAddOperation_zeroProbability() {
        builder.addOperation(Operation.OP1, 1.0)
               .addOperation(Operation.OP2, 0.0)
               .build();
    }

    @Test
    public void testAddOperation_zeroProbabilityAfterDefaultOperation() {
        builder.addDefaultOperation(Operation.DEFAULT)
               .addOperation(Operation.OP1, 0.0)
               .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddOperation_negativeProbability() {
        builder.addOperation(Operation.OP1, -0.1)
               .build();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperations_sumOfProbabilitiesExceedsLimit_NormalPrecision() {
        builder.addOperation(Operation.OP1, 0.8)
               .addOperation(Operation.OP2, 0.21)
               .build();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperations_sumOfProbabilitiesExceedsLimit_MaximumPrecision() {
        builder.addOperation(Operation.OP1, 0.8)
               .addOperation(Operation.OP2, 0.2 + OperationSelectorBuilder.PROBABILITY_INTERVAL)
               .build();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperations_sameOperationCannotBeAddedTwice() {
        builder.addOperation(Operation.OP1, 0.1)
               .addOperation(Operation.OP1, 0.1);
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperations_operationCannotBeAddedTwiceAsDefaultOperation() {
        builder.addOperation(Operation.OP1, 0.1)
               .addDefaultOperation(Operation.OP1);
    }

    @Test
    public void testAddOperations_maxPrecisionHitsLimit() {
        builder.addOperation(Operation.OP1, OperationSelectorBuilder.PROBABILITY_INTERVAL)
               .addDefaultOperation(Operation.DEFAULT)
               .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddOperations_maxPrecisionExceeded() {
        builder.addOperation(Operation.OP1, OperationSelectorBuilder.PROBABILITY_INTERVAL / 2)
               .addDefaultOperation(Operation.DEFAULT)
               .build();
    }

    @Test
    public void testAddOperations_floatingPointMadness() {
        // these probabilities add up to 1.0000000000000002
        double prob1 = 0.4;
        double prob2 = 0.2;
        double prob3 = 0.15;
        double prob4 = 0.2;
        double prob5 = 0.05;

        builder.addOperation(Operation.OP1, prob1)
                .addOperation(Operation.OP2, prob2)
                .addOperation(Operation.OP3, prob3)
                .addOperation(Operation.OP4, prob4)
                .addOperation(Operation.OP5, prob5)
                .build();
    }

    @Test
    public void testAddOperations_justDefaultOperation() {
        builder.addDefaultOperation(Operation.DEFAULT).build();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperation_afterBuild() {
        builder.addDefaultOperation(Operation.DEFAULT).build();
        builder.addOperation(Operation.OP1, 0.0);
    }

    @Test
    public void testGetOperations() {
        assertEquals(0, builder.getOperations().size());

        builder.addOperation(Operation.OP1, 0.1);
        assertEquals(1, builder.getOperations().size());

        builder.addOperation(Operation.OP2, 0.1);
        assertEquals(2, builder.getOperations().size());

        builder.addDefaultOperation(Operation.OP3);
        assertEquals(3, builder.getOperations().size());
    }

    @Test
    public void testSelect_defaultOperations() {
        double op1Probability = 0.1;
        double op2Probability = 0.1;

        selector = builder.addOperation(Operation.OP1, op1Probability)
                          .addOperation(Operation.OP2, op2Probability)
                          .addDefaultOperation(Operation.DEFAULT)
                          .build();

        Map<Operation, Integer> opsStats = exerciseSelector(selector);
        Integer op1Count = opsStats.get(Operation.OP1);
        Integer op2Count = opsStats.get(Operation.OP2);
        Integer defaultCount = opsStats.get(Operation.DEFAULT);
        assertCountIsWithinTolerance(Operation.OP1, op1Count, op1Probability);
        assertCountIsWithinTolerance(Operation.OP2, op2Count, op2Probability);
        assertCountIsWithinTolerance(Operation.DEFAULT, defaultCount, 1.0 - (op1Probability + op2Probability));
    }

    @Test
    public void testSelect_distributionIsAccordingToProbabilities() {
        double op1Probability = 0.70;
        double op2Probability = 0.29;
        double op3Probability = 0.01;

        selector = builder.addOperation(Operation.OP1, op1Probability)
                          .addOperation(Operation.OP2, op2Probability)
                          .addOperation(Operation.OP3, op3Probability)
                          .build();

        Map<Operation, Integer> opsStats = exerciseSelector(selector);
        Integer op1Count = opsStats.get(Operation.OP1);
        Integer op2Count = opsStats.get(Operation.OP2);
        Integer op3Count = opsStats.get(Operation.OP3);
        assertCountIsWithinTolerance(Operation.OP1, op1Count, op1Probability);
        assertCountIsWithinTolerance(Operation.OP2, op2Count, op2Probability);
        assertCountIsWithinTolerance(Operation.OP3, op3Count, op3Probability);
    }

    private void assertCountIsWithinTolerance(Operation op, int count, double probability) {
        double lowerBound = (ITERATIONS * probability - ITERATIONS * TOLERANCE);
        double upperBound = (ITERATIONS * probability + ITERATIONS * TOLERANCE);

        assertTrue(format("Operations %s was selected %d times, but lower bound is %f", op, count, lowerBound),
                count > lowerBound);
        assertTrue(format("Operations %s was selected %d times, but upper bound is %f", op, count, upperBound),
                count < upperBound);
    }

    private Map<Operation, Integer> exerciseSelector(OperationSelector<Operation> selector) {
        Map<Operation, Integer> opsStats = new HashMap<Operation, Integer>();
        for (int i = 0; i < ITERATIONS; i++) {
            Operation operation = selector.select();
            Integer counter = opsStats.get(operation);
            counter = (counter == null ? 1 : ++counter);
            opsStats.put(operation, counter);
        }
        return opsStats;
    }
}
