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
package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillisThrowException;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    public static File createTmpDirectory() {
        try {
            File dir = File.createTempFile("temp", "tmp-" + UuidUtil.newUnsecureUuidString());

            if (!dir.delete()) {
                throw new UncheckedIOException("Failed to delete temp file '" + dir.getAbsolutePath() + "'");
            }

            if (!dir.mkdir()) {
                throw new UncheckedIOException("Failed to create temp directory '" + dir.getAbsolutePath() + "'");
            }

            dir.deleteOnExit();
            return dir;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
     * <p>
     * This method makes use of an exponential back-off mechanism. So initially it will ask frequently, but the
     * more times it fails the less frequent the task is going to be retried.
     *
     * @param task           AssertTask to execute
     * @param timeoutSeconds timeout for assert in seconds
     * @throws NullPointerException if task is null.
     */
    public static void assertTrueEventually(AssertTask task, long timeoutSeconds) {
        checkNotNull(task, "task can't be null");

        AssertionError error;

        // the total timeout in ms
        long timeoutMs = SECONDS.toMillis(timeoutSeconds);

        // the time in ms when the assertTrue is going to expire
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
            // we put a cap on the maximum timeout.
            sleepMillis = (int) Math.min(SECONDS.toMillis(1), sleepMillis * ASSERT_TRUE_EVENTUALLY_SLEEP_FACTOR);
        }
    }

    /**
     * Assert that a certain task is going to assert to true eventually.
     * <p>
     * This method makes use of an exponential back-off mechanism. So initially it will ask frequently, but the
     * more times it fails the less frequent the task is going to be retried.
     * <p>
     * Uses the default timeout of {@link #ASSERT_TRUE_EVENTUALLY_TIMEOUT} milliseconds.
     *
     * @param task AssertTask to execute
     * @throws NullPointerException if task is null.
     */
    public static void assertTrueEventually(AssertTask task) {
        assertTrueEventually(task, ASSERT_TRUE_EVENTUALLY_TIMEOUT);
    }

    public static void assertCompletesEventually(final Future f) {
        assertTrueEventually(() -> assertTrue("future has not completed", f.isDone()));
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

    public static void assertNoExceptions() {
        File userDir = getUserDir();
        if (userDir.exists()) {
            for (File file : userDir.listFiles()) {
                if (file.isDirectory()) {
                    continue;
                }

                assertFalse("exception found: " + file + " content: " + fileAsText(file), file.getName().endsWith(".exception"));
            }
        }
    }

    public static void assertException(String content, int id) {
        File userDir = getUserDir();
        if (!userDir.exists()) {
            fail("userDir " + userDir.getAbsolutePath() + " does not exist");
        }

        File exceptionFile = new File(userDir, id + ".exception");
        assertTrue(exceptionFile.getAbsolutePath() + " does not exist", exceptionFile.exists());

        String text = fileAsText(exceptionFile);
        assertTrue(format("'%s' does not contains '%s'", text, content), text.contains(content));
    }
}
