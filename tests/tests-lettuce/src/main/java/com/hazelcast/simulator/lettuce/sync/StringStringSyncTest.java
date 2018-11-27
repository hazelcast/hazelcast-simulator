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
package com.hazelcast.simulator.lettuce.sync;

import com.hazelcast.simulator.lettuce.LettuceTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateStrings;

public class StringStringSyncTest extends LettuceTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;

    private String[] values;

    @Setup
    public void setup() {
        values = generateStrings(valueCount, minValueLength, maxValueLength);
    }

    // loading the data is very inefficient. Needs some work in the future
    @Prepare(global = true)
    public void loadInitialData() {
        Random random = new Random();
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands sync = connection.sync();
        for (int k = 0; k < keyDomain; k++) {
            int r = random.nextInt(valueCount);
            sync.set(Long.toString(k), values[r]);
        }
    }

    @TimeStep(prob = -1)
    public String get(ThreadState state) {
        return state.sync.get(state.randomKey());
    }

    @TimeStep(prob = 1)
    public String put(ThreadState state) {
        return state.sync.set(state.randomKey(), state.randomValue());
    }

    public class ThreadState extends BaseThreadState {
        final RedisCommands<String, String> sync;

        ThreadState() {
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            sync = connection.sync();
        }

        private String randomKey() {
            return Long.toString(randomLong(keyDomain));
        }

        private String randomValue() {
            return values[randomInt(values.length)];
        }
    }
}
