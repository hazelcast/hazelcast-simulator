package com.hazelcast.stabilizer.worker;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class OperationSelectorTest {
    private static final int ITERATIONS = 1000000;
    private static final double TOLERANCE = 0.05;

    private OperationSelector<MyOps> selector;

    @Before
    public void setUp() {
        selector = new OperationSelector<MyOps>();
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperations_sumOfProbabilitiesCannotExceed1() {
        selector.addOperation(MyOps.OP1, 0.8)
                .addOperation(MyOps.OP2, 0.8);
    }

    @Test(expected = IllegalStateException.class)
    public void testAddOperations_operationCannotBeAddedTwice() {
        selector.addOperation(MyOps.OP1, 0.1)
                .addOperation(MyOps.OP1, 0.1);
    }

    @Test
    public void testSelect_emptyOperations() {
        double op1Prob = 0.1;
        double op2Prob = 0.1;

        selector.addOperation(MyOps.OP1, op1Prob)
                .addOperation(MyOps.OP2, op2Prob)
                .empty(MyOps.EMPTY);

        Map<MyOps, Integer> opsStats = exerciseSelector(selector);
        Integer op1Count = opsStats.get(MyOps.OP1);
        Integer op2Count = opsStats.get(MyOps.OP2);
        Integer emptyCount = opsStats.get(MyOps.EMPTY);
        assertCountIsWithinTolerance(MyOps.OP1, op1Count, op1Prob);
        assertCountIsWithinTolerance(MyOps.OP2, op2Count, op2Prob);
        assertCountIsWithinTolerance(MyOps.EMPTY, emptyCount, 1 - (op1Prob + op2Prob));
    }

    @Test
    public void testSelect_distributionIsAccordingToProbabilities() {
        double op1Prob = 0.5;
        double op2Prob = 0.3;
        double op3Prob = 0.2;

        selector.addOperation(MyOps.OP1, op1Prob)
                .addOperation(MyOps.OP2, op2Prob)
                .addOperation(MyOps.OP3, op3Prob);
        Map<MyOps, Integer> opsStats = exerciseSelector(selector);

        Integer op1Count = opsStats.get(MyOps.OP1);
        Integer op2Count = opsStats.get(MyOps.OP2);
        Integer op3Count = opsStats.get(MyOps.OP3);
        assertCountIsWithinTolerance(MyOps.OP1, op1Count, op1Prob);
        assertCountIsWithinTolerance(MyOps.OP2, op2Count, op2Prob);
        assertCountIsWithinTolerance(MyOps.OP3, op3Count, op3Prob);
    }

    private void assertCountIsWithinTolerance(MyOps op, int count, double probability) {
        double lowerBound = (ITERATIONS * probability - ITERATIONS * TOLERANCE);
        double upperBound = (ITERATIONS * probability + ITERATIONS * TOLERANCE);

        assertTrue("Operations " + op + " was selected " + count + " times, however lower bound is " + lowerBound,
                count > lowerBound);
        assertTrue("Operations " + op + " was selected " + count + " times, however upper bound is " + upperBound,
                count < upperBound);
    }

    private Map<MyOps, Integer> exerciseSelector(OperationSelector<MyOps> selector) {
        Map<MyOps, Integer> opsStats = new HashMap<MyOps, Integer>();
        for (int i = 0 ; i < ITERATIONS; i++) {
            MyOps myOps = selector.select();
            Integer counter = opsStats.get(myOps);
            counter = (counter == null ? 1 : ++counter);
            opsStats.put(myOps, counter);
        }
        return opsStats;
    }

    private static enum MyOps {
        OP1, OP2, OP3, EMPTY
    }
}
