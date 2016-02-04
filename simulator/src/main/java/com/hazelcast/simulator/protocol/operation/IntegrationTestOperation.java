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

/**
 * Operation for integration tests of the Simulator Protocol.
 */
public class IntegrationTestOperation implements SimulatorOperation {

    public enum Operation {
        EQUALS,
        NESTED_SYNC,
        NESTED_ASYNC,
        DEEP_NESTED_SYNC,
        DEEP_NESTED_ASYNC
    }

    public static final String TEST_DATA = "IntegrationTestData";

    private final String operation;
    private final String testData;

    public IntegrationTestOperation() {
        this(Operation.EQUALS, TEST_DATA);
    }

    public IntegrationTestOperation(Operation operation) {
        this(operation, null);
    }

    public IntegrationTestOperation(Operation operation, String testData) {
        this.operation = operation.name();
        this.testData = testData;
    }

    public Operation getOperation() {
        return Operation.valueOf(operation);
    }

    public String getTestData() {
        return testData;
    }
}
