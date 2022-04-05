package com.hazelcast.simulator.worker.testcontainer;

import org.junit.Test;

import java.util.Arrays;

import static com.hazelcast.simulator.worker.testcontainer.Probability.methodProbabilitiesToMethodRatios;
import static com.hazelcast.simulator.worker.testcontainer.Probability.ratiosToMethodProbabilityArray;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ProbabilityTest {

    @Test
    public void testMagic_singleItem() {
        int[] x = methodProbabilitiesToMethodRatios(1);
        // assertNull(x);
    }

    @Test
    public void test_methodProbabilitiesToMethodRatios() {
        int[] ratios;

        ratios = methodProbabilitiesToMethodRatios(0.1, 0.2, 0.1, 0.29, 0.21, 0.1);
        assertEquals(6, ratios.length);
        assertEquals(10, ratios[0]);
        assertEquals(20, ratios[1]);
        assertEquals(10, ratios[2]);
        assertEquals(29, ratios[3]);
        assertEquals(21, ratios[4]);
        assertEquals(10, ratios[5]);
    }

    @Test
    public void test_methodProbabilitiesToMethodRatios_highPricision() {
        int[] ratios;

        ratios = methodProbabilitiesToMethodRatios(0.1, 0.9);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(9, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.01, 0.99);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(99, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.001, 0.999);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(999, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.0001, 0.9999);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(9999, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.00001, 0.99999);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(99999, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.000001, 0.999999);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(999999, ratios[1]);
    }

    @Test
    public void test_methodProbabilitiesToMethodRatios_simplication() {
        int[] ratios;

        ratios = methodProbabilitiesToMethodRatios(0.10, 0.90);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(9, ratios[1]);


        ratios = methodProbabilitiesToMethodRatios(0.1, 0.90);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(9, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.100, 0.900);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(9, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.1000, 0.9000);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(9, ratios[1]);

        ratios = methodProbabilitiesToMethodRatios(0.100000, 0.900000);
        assertEquals(2, ratios.length);
        assertEquals(1, ratios[0]);
        assertEquals(9, ratios[1]);
    }


    @Test
    public void toMethodProbabilityArray() {
        assertMethodProbabilityArray(1, 99);
        assertMethodProbabilityArray(10, 90);
        assertMethodProbabilityArray(1, 900, 100, 9, 99);
    }

    public void assertMethodProbabilityArray(int... ratios) {
        byte[] probabilityArray = ratiosToMethodProbabilityArray(ratios);

        int[] remainingMethodRatios = Arrays.copyOf(ratios, ratios.length);

        for (int k = 0; k < probabilityArray.length; k++) {
            int methodIndex = probabilityArray[k];
            if (methodIndex >= remainingMethodRatios.length) {
                fail("Unknown method " + methodIndex);
            }

            remainingMethodRatios[methodIndex]--;
        }

        // make sure that everything is 0
        for (int methodIndex = 0; methodIndex < remainingMethodRatios.length; methodIndex++) {
            if (remainingMethodRatios[methodIndex] != 0) {
                fail(format("remaining ratio [%s] for method [%s]", remainingMethodRatios[methodIndex], methodIndex));
            }
        }
    }
}