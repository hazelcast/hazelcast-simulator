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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.teardownFakeUserDir;
import static com.hazelcast.simulator.utils.ExceptionReporter.report;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ExceptionReporterTest {

    @Before
    public void before() {
        setupFakeUserDir();
        ExceptionReporter.reset();
    }

    @After
    public void after() {
        teardownFakeUserDir();
        ExceptionReporter.reset();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ExceptionReporter.class);
    }

    @Test
    public void testReportNullCause_thenIgnored() {
        report("testID", null);

        File exceptionFile = new File(getUserDir(), "1.exception");
        assertFalse(exceptionFile.exists());
    }

    @Test
    public void testReport() {
        report("testID", new RuntimeException("Expected exception"));

        File exceptionFile = new File(getUserDir(), "1.exception");
        assertTrue(exceptionFile.exists());
        assertFalse(new File(getUserDir(), "1.exception.tmp").exists());
        assertNotNull(fileAsText(exceptionFile));
    }

    @Test
    public void testReportTooManyExceptions() {
        int currentExceptions = ExceptionReporter.MAX_EXCEPTION_COUNT + 1;
        ExceptionReporter.FAILURE_ID.set(currentExceptions);
        report("testID", new RuntimeException("Expected exception"));

        // make sure no new files have been created
        File userDir = getUserDir();
        assertNotNull(userDir);
        assertEquals(0, userDir.listFiles().length);
    }
}
