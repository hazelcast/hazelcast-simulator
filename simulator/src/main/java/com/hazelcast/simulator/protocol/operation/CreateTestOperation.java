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
package com.hazelcast.simulator.protocol.operation;

import com.hazelcast.simulator.test.TestCase;

import java.util.Map;

/**
 * Creates a new Simulator test with the defined properties.
 */
public class CreateTestOperation implements SimulatorOperation {

    private final int testIndex;
    private final String testId;
    private final Map<String, String> properties;

    public CreateTestOperation(int testIndex, TestCase testCase) {
        this.testIndex = testIndex;
        this.testId = testCase.getId();
        this.properties = testCase.getProperties();
    }

    public int getTestIndex() {
        return testIndex;
    }

    public TestCase getTestCase() {
        return new TestCase(testId, properties);
    }

    @Override
    public String toString() {
        return "CreateTestOperation{"
                + "testIndex='" + testIndex + '\''
                + ", testId='" + testId + '\''
                + '}';
    }
}
