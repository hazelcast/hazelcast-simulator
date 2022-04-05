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
package com.hazelcast.simulator.tests.special;

import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;

/**
 * Simple test that runs for a number of iterations.
 *
 * Each timestep thread will run for the number of iterations.
 */
public class IterationTest extends HazelcastTest {

    public int iterationCount = 1000000;
    public boolean exceptionOnExit;

    @BeforeRun
    public void beforeRun(ThreadState state) {
        state.count = iterationCount;
    }

    @TimeStep
    public void timeStep(ThreadState state) {
        if (state.count == 0) {
            if (exceptionOnExit) {
                throw new RuntimeException("Expected exception");
            } else {
                throw new StopException();
            }
        }

        state.count--;
    }

    public class ThreadState extends BaseThreadState {
        int count;
    }
}
