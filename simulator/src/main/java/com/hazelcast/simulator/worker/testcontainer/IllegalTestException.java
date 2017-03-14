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
package com.hazelcast.simulator.worker.testcontainer;

/**
 * Exception thrown when a test is not valid, e.g. it has no method with a {@link com.hazelcast.simulator.test.annotations.Run}
 * or {@link com.hazelcast.simulator.test.annotations.TimeStep} annotation.
 */
public class IllegalTestException extends RuntimeException {

    public IllegalTestException(String message) {
        super(message);
    }

    public IllegalTestException(String message, Throwable cause) {
        super(message, cause);
    }
}
