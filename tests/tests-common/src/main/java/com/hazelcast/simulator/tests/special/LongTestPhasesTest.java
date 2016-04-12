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

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;

import static com.hazelcast.simulator.test.TestPhase.GLOBAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.GLOBAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_TEARDOWN;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_VERIFY;
import static com.hazelcast.simulator.test.TestPhase.LOCAL_WARMUP;
import static com.hazelcast.simulator.test.TestPhase.RUN;
import static com.hazelcast.simulator.test.TestPhase.SETUP;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

/**
 * Used to test timeout detection of {@link com.hazelcast.simulator.agent.workerjvm.WorkerJvmFailureMonitor}.
 */
public class LongTestPhasesTest {

    // properties
    public boolean allPhases = false;
    public TestPhase testPhase = RUN;
    public int sleepSeconds = 120;

    @Setup
    public void setUp(TestContext testContext) {
        sleep(SETUP);
    }

    @Warmup(global = false)
    public void localWarmup() {
        sleep(LOCAL_WARMUP);
    }

    @Warmup(global = true)
    public void globalWarmup() {
        sleep(GLOBAL_WARMUP);
    }

    @Run
    public void run() {
        sleep(RUN);
    }

    @Verify(global = true)
    public void globalVerify() {
        sleep(GLOBAL_VERIFY);
    }

    @Verify(global = false)
    public void localVerify() {
        sleep(LOCAL_VERIFY);
    }

    @Teardown(global = true)
    public void globalTearDown() {
        sleep(GLOBAL_TEARDOWN);
    }

    @Teardown(global = false)
    public void localTearDown() {
        sleep(LOCAL_TEARDOWN);
    }

    private void sleep(TestPhase currentTestPhase) {
        if (allPhases || currentTestPhase.equals(testPhase)) {
            sleepSeconds(sleepSeconds);
        }
    }
}
