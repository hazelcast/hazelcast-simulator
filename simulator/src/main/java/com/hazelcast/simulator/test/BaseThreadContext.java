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

import java.util.Random;

/**
 * Implementations of this class should be public. This is required for the generated worker class to work optimally.
 *
 * Why is this class called BaseThreadContext and not ThreadContext? In most cases a test needs to subclass this
 * BaseThreadContext; and in this case they can use the prettier name 'ThreadContext'.
 */
public class BaseThreadContext {

    private final Random random = new Random();

    public Random getRandom() {
        return random;
    }

    public long randomLong() {
        return random.nextLong();
    }

    public int randomInt() {
        return random.nextInt();
    }

    public int randomInt(int bound) {
        return random.nextInt(bound);
    }

    public boolean randomBoolean() {
        return random.nextBoolean();
    }

    public long iteration() {
        return 0;
    }
}
