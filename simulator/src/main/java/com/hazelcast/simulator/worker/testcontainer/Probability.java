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
package com.hazelcast.simulator.worker.testcontainer;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static java.lang.Math.round;

public class Probability {
    public static final int PROBABILITY_LENGTH = 1000 * 1000;
    public static final int TEN = 10;

    private final double value;

    public Probability(double probability) {
        this.value = probability;
    }

    public boolean isLargerThanZero() {
        return toInt(value) > toInt(0);
    }

    public boolean isLargerThanOne() {
        return toInt(value) > toInt(1);
    }

    public boolean isSmallerThanZero() {
        return toInt(value) < toInt(0);
    }

    public boolean isSmallerThanOne() {
        return toInt(value) < toInt(1);
    }

    public boolean isMinusOne() {
        return toInt(value) == toInt(-1);
    }

    public Probability add(Probability that) {
        return new Probability(this.value + that.value);
    }

    public Probability sub(Probability that) {
        return new Probability(this.value - that.value);
    }

    private int toInt(double v) {
        return (int) round(v * PROBABILITY_LENGTH);
    }

    public double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public static byte[] loadTimeStepProbabilityArray(Map<Method, Probability> methods, List<Method> activeMethods) {
        if (activeMethods.size() < 2) {
            return null;
        }

        double[] methodProbabilities = new double[activeMethods.size()];
        for (int methodIndex = 0; methodIndex < activeMethods.size(); methodIndex++) {
            Method method = activeMethods.get(methodIndex);
            Probability probability = methods.get(method);
            methodProbabilities[methodIndex] = probability.getValue();
        }

        int[] methodRatios = methodProbabilitiesToMethodRatios(methodProbabilities);
        return ratiosToMethodProbabilityArray(methodRatios);
    }

    public static int[] methodProbabilitiesToMethodRatios(double... methodProbabilities) {
        int[] roundedMethodProbabilities = new int[methodProbabilities.length];

        for (int k = 0; k < methodProbabilities.length; k++) {
            roundedMethodProbabilities[k] = (int) Math.round(methodProbabilities[k] * PROBABILITY_LENGTH);
        }

        // now we simplify
        for (; ; ) {
            boolean powerOfTen = true;
            for (int k = 0; k < roundedMethodProbabilities.length; k++) {
                if (roundedMethodProbabilities[k] % TEN != 0) {
                    powerOfTen = false;
                    break;
                }
            }

            if (!powerOfTen) {
                break;
            }

            for (int k = 0; k < roundedMethodProbabilities.length; k++) {
                roundedMethodProbabilities[k] = roundedMethodProbabilities[k] / TEN;
            }
        }

        return roundedMethodProbabilities;
    }

    public static byte[] ratiosToMethodProbabilityArray(int... methodRatios) {
        int length = 0;
        for (int methodIndex = 0; methodIndex < methodRatios.length; methodIndex++) {
            length += methodRatios[methodIndex];
        }

        byte[] bytes = new byte[length];
        int index = 0;
        for (int methodIndex = 0; methodIndex < methodRatios.length; methodIndex++) {
            for (int x = 0; x < methodRatios[methodIndex]; x++) {
                bytes[index] = (byte) methodIndex;
                index++;
            }
        }

        return bytes;
    }
}
