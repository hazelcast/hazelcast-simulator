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
package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.TestException;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Verify;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.fail;

public class FailingTest extends AbstractTest{

    @Prepare
    public void prepare() {
        sleepSeconds(1);
    }

    @Verify
    public void verify() {
        fail("Expected exception in verify method");
    }

    public int count = 0;

    @Run
    public void run() {
        if (!testContext.isStopped()) {
            sleepSeconds(1);
            count++;
            throw new TestException("This test should fail: "+count);
        }
    }
}
