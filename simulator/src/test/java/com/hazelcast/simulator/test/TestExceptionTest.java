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
package com.hazelcast.simulator.test;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestExceptionTest {

    @Test
    public void testConstructor_withCause() {
        RuntimeException cause = new RuntimeException();
        TestException exception = new TestException(cause);

        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testConstructor_withMessage() {
        TestException exception = new TestException("cause");

        assertEquals("cause", exception.getMessage());
    }

    @Test
    public void testConstructor_withMessageFormat_singleArgument() {
        TestException exception = new TestException("cause %d", 1);

        assertEquals("cause 1", exception.getMessage());
    }

    @Test
    public void testConstructor_withMessageFormat_multipleArguments() {
        TestException exception = new TestException("cause %d %d %s", 1, 2, "3");

        assertEquals("cause 1 2 3", exception.getMessage());
    }

    @Test
    public void testConstructor_withMessageFormat_withException() {
        Throwable cause = new RuntimeException();
        TestException exception = new TestException("cause %d %d %s", 1, 2, "3", cause);

        assertEquals("cause 1 2 3", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
