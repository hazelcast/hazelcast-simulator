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
package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class SuccessTest {

    private TestContext context;

    @Setup
    public void setUp(TestContext context) {
        this.context = context;
    }

    @Teardown(global = false)
    public void localTearDown() {
    }

    @Teardown(global = true)
    public void globalTearDown() {
    }

    @Prepare(global = false)
    public void localPrepare() {
    }

    @Prepare(global = true)
    public void globalPrepare() {
    }

    @Verify(global = false)
    public void localVerify() {
    }

    @Verify(global = true)
    public void globalVerify() {
    }

    @Run
    public void run() {
        while (!context.isStopped()) {
            sleepSeconds(1);
            System.out.println("testSuccess running");
        }
        System.out.println("testSuccess completed");
    }
}
