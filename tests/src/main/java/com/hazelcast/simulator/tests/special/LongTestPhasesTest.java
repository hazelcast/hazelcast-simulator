/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

/**
 * Used to test timeout detection of {@link com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor}.
 */
public class LongTestPhasesTest {

    // properties
    public boolean allPhases = false;
    public int sleepSeconds = 120;

    @Setup
    public void setUp(TestContext testContext) {
        sleepConditional();
    }

    @Teardown
    public void tearDown() {
        sleepConditional();
    }

    @Warmup(global = false)
    public void warmup() {
        sleepConditional();
    }

    @Verify(global = false)
    public void verify() {
        sleepConditional();
    }

    @Run
    public void run() {
        sleepSeconds(sleepSeconds);
    }

    private void sleepConditional() {
        if (allPhases) {
            sleepSeconds(sleepSeconds);
        }
    }
}
