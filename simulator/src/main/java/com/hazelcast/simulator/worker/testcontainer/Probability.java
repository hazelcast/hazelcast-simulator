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

import static java.lang.Math.pow;
import static java.lang.Math.round;

public class Probability {

    private static final int PROBABILITY_PRECISION = 3;
    private static final int PROBABILITY_LENGTH = (int) round(pow(10, PROBABILITY_PRECISION));

    private final double value;

    public Probability(double probability) {
        this.value = probability;
    }

    public double getValue() {
        return value;
    }

    public Probability add(Probability that) {
        return new Probability(this.value + that.value);
    }

    public Probability sub(Probability that) {
        return new Probability(this.value - that.value);
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

    @Override
    public String toString() {
        return Double.toString(value);
    }

    public static byte[] loadTimeStepProbabilityArray(Map<Method, Probability> methods, List<Method> activeMethods) {
        if (activeMethods.size() < 2) {
            return null;
        }

        byte[] result = new byte[PROBABILITY_LENGTH];
        int index = 0;

        for (int methodIndex = 0; methodIndex < activeMethods.size(); methodIndex++) {
            Method method = activeMethods.get(methodIndex);
            Probability probability = methods.get(method);
            for (int i = 0; i < round(probability.getValue() * result.length); i++) {
                if (index < result.length) {
                    result[index] = (byte) methodIndex;
                    index++;
                }
            }
        }
        return result;
    }

    private static int toInt(double v) {
        return (int) round(v * PROBABILITY_LENGTH);
    }
}
