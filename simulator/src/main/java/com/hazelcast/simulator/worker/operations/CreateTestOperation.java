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
package com.hazelcast.simulator.worker.operations;

import com.google.gson.annotations.SerializedName;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;

import java.util.Map;

/**
 * Creates a Simulator Test.
 *
 * In case of the java worker, a TestContainer is made, a Test-instance (e.g. AtomicLongTest) is made and the properties
 * are are bound.
 */
public class CreateTestOperation implements SimulatorOperation {

     /**
     * Test id for for {@link TestCase}.
     */
    @SerializedName("testId")
    private final String testId;

    /**
     * Test parameters which are injected to public variables of the same name.
     *
     * This is the content of the testCase.
     */
    @SerializedName("properties")
    private final Map<String, String> properties;

    public CreateTestOperation(TestCase testCase) {
        this.testId = testCase.getId();
        this.properties = testCase.getProperties();
    }

    public TestCase getTestCase() {
        return new TestCase(testId, properties);
    }

    @Override
    public String toString() {
        return "CreateTestOperation{testId='" + testId + "'}";
    }
}
