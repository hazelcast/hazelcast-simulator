/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.helpers;

import java.io.Serializable;
import java.util.Random;

/**
 * Helper class, holds a key and an amount to increment by.
 * Also useful for printing out data in nice format.
 */
public class KeyIncrementPair implements Serializable {

    public final int key;
    public final int increment;

    public KeyIncrementPair(Random random, int maxKey, int maxIncrement) {
        key = random.nextInt(maxKey);
        increment = random.nextInt(maxIncrement - 1) + 1;
    }

    @Override
    public String toString() {
        return "KeyIncrementPair{"
                + "key=" + key
                + ", increment=" + increment
                + '}';
    }
}
