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
package com.hazelcast.simulator.test;

import java.io.Serializable;
import java.util.Random;

/**
 * Implementations of this class should be public. This is required for the generated TimeStepLoop class to work.
 * <p>
 * This class is called BaseThreadState instead of ThreadState, since in most cases a test needs to subclass BaseThreadState.
 * In this case the prettier and shorter name ThreadState can be used.
 */
@SuppressWarnings("unused")
public class BaseThreadState implements Serializable {

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final Random random = new Random();

    /**
     * @return random generated double
     */
    public double randomDouble() {
        return random.nextDouble();
    }

    /**
     * @return random generated long
     */
    public long randomLong() {
        return random.nextLong();
    }

    /**
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return random generated long within a given bound.
     * @throws IllegalArgumentException if bound smaller or equal than 0.
     */
    public long randomLong(long bound) {
        if (bound <= 0) {
            throw new IllegalArgumentException("bound must be positive");
        }

        // rounding will always be down towards 0. So the result will be exclusive the bound.
        return (long) (random.nextDouble() * bound);
    }

    /**
     * @return random generated int
     */
    public int randomInt() {
        return random.nextInt();
    }

    /**
     * @param bound the upper bound (exclusive).  Must be positive.
     * @return random generated int within a given bound.
     * @throws IllegalArgumentException if bound smaller or equal than 0.
     */
    public int randomInt(int bound) {
        return random.nextInt(bound);
    }

    /**
     * @return random generated boolean
     */
    public boolean randomBoolean() {
        return random.nextBoolean();
    }
}
