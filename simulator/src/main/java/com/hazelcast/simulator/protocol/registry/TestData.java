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
package com.hazelcast.simulator.protocol.registry;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.test.TestCase;

public class TestData {

    private final int testIndex;
    private final SimulatorAddress address;
    private final TestCase testCase;

    public TestData(int testIndex, SimulatorAddress address, TestCase testCase) {
        this.testIndex = testIndex;
        this.address = address;
        this.testCase = testCase;
    }

    public int getTestIndex() {
        return testIndex;
    }

    public SimulatorAddress getAddress() {
        return address;
    }

    public TestCase getTestCase() {
        return testCase;
    }
}
