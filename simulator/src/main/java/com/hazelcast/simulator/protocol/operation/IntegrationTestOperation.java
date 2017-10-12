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
package com.hazelcast.simulator.protocol.operation;

/**
 * Executes tasks for integration tests of the Simulator Protocol.
 */
public class IntegrationTestOperation implements SimulatorOperation {

    public enum Type {
        EQUALS,
        NESTED_SYNC,
        NESTED_ASYNC,
        DEEP_NESTED_SYNC,
        DEEP_NESTED_ASYNC
    }

    /**
     * Test data which is used for the {@link Type#EQUALS} type.
     */
    public static final String TEST_DATA = "IntegrationTestData";

    /**
     * Defines the {@link Type} of this operation.
     */
    private final String type;

    /**
     * Defines the payload of this operation.
     */
    private final String testData;

    public IntegrationTestOperation() {
        this(Type.EQUALS, TEST_DATA);
    }

    public IntegrationTestOperation(Type type) {
        this(type, null);
    }

    public IntegrationTestOperation(Type type, String testData) {
        this.type = type.name();
        this.testData = testData;
    }

    public Type getType() {
        return Type.valueOf(type);
    }

    public String getTestData() {
        return testData;
    }
}
