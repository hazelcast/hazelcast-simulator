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
package com.hazelcast.simulator.benchmarks;

import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.Random;

public class BaseTimeBenchmark {


    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    @TimeStep(prob = 0.5)
    public void noopTimeStep() throws Exception {

    }

    @TimeStep(prob = 0.5)
    public void randomIntTimeStamp() throws Exception {
        int randomInt = new Random().nextInt(entryCount);
    }
}
