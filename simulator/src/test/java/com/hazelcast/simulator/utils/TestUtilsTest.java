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
package com.hazelcast.simulator.utils;

import org.junit.Test;

import java.util.IllegalFormatConversionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static com.hazelcast.simulator.utils.TestUtils.getUserContextKeyFromTestId;
import static com.hazelcast.simulator.utils.TestUtils.printAllStackTraces;
import static java.lang.String.format;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(TestUtils.class);
    }

    @Test
    public void testGetUserContextKeyFromTestId() {
        String actual = getUserContextKeyFromTestId("AnyTestCaseId");
        assertNotNull(actual);
        assertTrue(actual.contains("AnyTestCaseId"));
    }

    @Test(expected = IllegalFormatConversionException.class)
    public void testAssertEqualsStringFormatIllegalFormatConversionException() {
        assertEqualsStringFormat("%d %d", "42", "42");
    }

    @Test
    public void testAssertEqualsStringFormat() {
        assertEqualsStringFormat("%d %d", 23, 23);
    }

    @Test(expected = AssertionError.class)
    public void testAssertEqualsStringFormatAssertionError() {
        assertEqualsStringFormat("%d %d", 23, 42);
    }

    @Test
    public void testAssertEqualsStringFormatDouble() {
        assertEqualsStringFormat("%f %f", 23.42d, 23.42d, 0.01);
    }

    @Test
    public void testAssertEqualsStringFormatDoubleDelta() {
        assertEqualsStringFormat("%f %f", 23.421d, 23.422d, 0.01);
    }

    @Test(expected = NullPointerException.class)
    public void testAssertTrueEventuallyNull() {
        assertTrueEventually(null);
    }

    @Test
    public void testAssertTrueEventually() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final AtomicInteger assertTaskAttemptsCounter = new AtomicInteger(0);
        final AtomicInteger assertTaskSuccessCounter = new AtomicInteger(0);

        Thread countDownThread = new Thread() {
            @Override
            public void run() {
                sleepSeconds(3);
                countDownLatch.countDown();
            }
        };
        countDownThread.start();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTaskAttemptsCounter.incrementAndGet();
                assertTrue(countDownLatch.getCount() < 1);
                assertTaskSuccessCounter.incrementAndGet();
            }
        });

        countDownThread.interrupt();
        countDownThread.join();

        int attempts = assertTaskAttemptsCounter.get();
        assertTrue(format("Expected assertTaskAttemptsCounter > 1, but was %d", attempts), attempts > 1);

        int success = assertTaskSuccessCounter.get();
        assertTrue(format("Expected assertTaskSuccessCounter == 1, but was %d", success), success == 1);
    }

    @Test(expected = RuntimeException.class)
    public void testAssertTrueEventuallyException() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                throw new Exception("Expected exception");
            }
        });
    }

    @Test(expected = AssertionError.class)
    public void testAssertTrueEventuallyTimeOut() {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                fail("Expected failure");
            }
        }, TimeUnit.SECONDS.toSeconds(2));
    }

    @Test
    public void testPrintAllStackTraces() {
        printAllStackTraces();
    }
}
