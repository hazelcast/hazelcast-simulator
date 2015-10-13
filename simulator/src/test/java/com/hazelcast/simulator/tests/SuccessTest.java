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
package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestPhase;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class SuccessTest {

    private final Set<TestPhase> testPhases = new HashSet<TestPhase>();

    private TestContext context;

    public Set<TestPhase> getTestPhases() {
        return testPhases;
    }

    @Setup
    public void setUp(TestContext context) {
        this.context = context;
        testPhases.add(TestPhase.SETUP);
    }

    @Teardown(global = false)
    public void localTearDown() {
        testPhases.add(TestPhase.LOCAL_TEARDOWN);
    }

    @Teardown(global = true)
    public void globalTearDown() {
        testPhases.add(TestPhase.GLOBAL_TEARDOWN);
    }

    @Warmup(global = false)
    public void localWarmup() {
        sleepSeconds(1);
        testPhases.add(TestPhase.LOCAL_WARMUP);
    }

    @Warmup(global = true)
    public void globalWarmup() {
        sleepSeconds(1);
        testPhases.add(TestPhase.GLOBAL_WARMUP);
    }

    @Verify(global = false)
    public void localVerify() {
        testPhases.add(TestPhase.LOCAL_VERIFY);
    }

    @Verify(global = true)
    public void globalVerify() {
        testPhases.add(TestPhase.GLOBAL_VERIFY);
    }

    @Run
    void run() {
        while (!context.isStopped()) {
            sleepSeconds(1);
            testPhases.add(TestPhase.RUN);
        }
    }
}
