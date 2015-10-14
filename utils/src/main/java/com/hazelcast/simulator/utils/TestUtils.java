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
package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public final class TestUtils {

    private static final int ASSERT_TRUE_EVENTUALLY_INITIAL_SLEEP_MILLIS = 100;
    private static final float ASSERT_TRUE_EVENTUALLY_SLEEP_FACTOR = 1.5f;
    private static final int ASSERT_TRUE_EVENTUALLY_TIMEOUT;

    private static final Logger LOGGER = Logger.getLogger(TestUtils.class);

    static {
        ASSERT_TRUE_EVENTUALLY_TIMEOUT = Integer.parseInt(System.getProperty(
                "hazelcast.assertTrueEventually.timeout", "300"));
    }

    private TestUtils() {
    }

    public static String getUserContextKeyFromTestId(String testId) {
        return "testInstance:" + testId;
    }

    /**
     * This method executes the normal assertEquals with expected and actual values.
     * In addition it formats the given string with those values to provide a good assert message.
     *
     * @param message  assert message which is formatted with expected and actual values
     * @param expected expected value which is used for assert
     * @param actual   actual value which is used for assert
     */
    public static void assertEqualsStringFormat(String message, Object expected, Object actual) {
        assertEquals(format(message, expected, actual), expected, actual);
    }

    /**
     * This method executes the normal assertEquals with expected and actual values.
     * In addition it formats the given string with those values to provide a good assert message.
     *
     * @param message  assert message which is formatted with expected and actual values
     * @param expected expected value which is used for assert
     * @param actual   actual value which is used for assert
     * @param delta    delta value for double comparison
     */
    public static void assertEqualsStringFormat(String message, Double expected, Double actual, Double delta) {
        assertEquals(format(message, expected, actual), expected, actual, delta);
    }

    /**
     * Assert that a certain task is going to assert to true eventually.
     *
     * This method makes use of an exponential back-off mechanism. So initially it will ask frequently, but the
     * more times it fails the less frequent the task is going to be retried.
     *
     * @param task           AssertTask to execute
     * @param timeoutSeconds timeout for assert in seconds
     * @throws NullPointerException if task is null.
     */
    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        if (task == null) {
            throw new NullPointerException("task can't be null");
        }

        AssertionError error;

        // the total timeout in ms.
        long timeoutMs = TimeUnit.SECONDS.toMillis(timeoutSeconds);

        // the time in ms when the assertTrue is going to expire.
        long expirationMs = System.currentTimeMillis() + timeoutMs;
        int sleepMillis = ASSERT_TRUE_EVENTUALLY_INITIAL_SLEEP_MILLIS;

        for (; ; ) {
            try {
                try {
                    task.run();
                } catch (Exception e) {
                    throw rethrow(e);
                }
                return;
            } catch (AssertionError e) {
                error = e;
            }

            // there is a timeout, so we are done
            if (System.currentTimeMillis() > expirationMs) {
                throw error;
            }

            sleepMillisThrowException(sleepMillis);
            sleepMillis *= ASSERT_TRUE_EVENTUALLY_SLEEP_FACTOR;
        }
    }

    /**
     * Assert that a certain task is going to assert to true eventually.
     *
     * This method makes use of an exponential back-off mechanism. So initially it will ask frequently, but the
     * more times it fails the less frequent the task is going to be retried.
     *
     * Uses the default timeout of {@link #ASSERT_TRUE_EVENTUALLY_TIMEOUT} milliseconds.
     *
     * @param task AssertTask to execute
     * @throws NullPointerException if task is null.
     */
    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void printAllStackTraces() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Thread, StackTraceElement[]> entry : Thread.getAllStackTraces().entrySet()) {
            sb.append("Thread ").append(entry.getKey().getName());
            for (StackTraceElement stackTraceElement : entry.getValue()) {
                sb.append("\tat ").append(stackTraceElement);
            }
        }
        LOGGER.error(sb.toString());
    }
}
