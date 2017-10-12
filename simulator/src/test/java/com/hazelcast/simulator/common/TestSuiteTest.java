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
package com.hazelcast.simulator.common;

import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.hazelcast.simulator.coordinator.TestSuite.loadTestSuite;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestSuiteTest {

    private File testSuiteFile;

    @Before
    public void before() {
        testSuiteFile = ensureExistingFile("simulator.properties");
    }

    @After
    public void after() {
        deleteQuiet(testSuiteFile);
    }

    @Test
    public void testConstructor() {
        TestSuite testSuite = new TestSuite();

        assertTrue(testSuite.getTestCaseList().isEmpty());
        assertEquals(0, testSuite.getMaxTestCaseIdLength());
    }

    @Test(expected = IllegalArgumentException.class)
    public void loadTestSuite_whenInvalidTestId() {
        String txt = "*@class=AtomicLong" + NEW_LINE;

        createTestSuite(txt);
    }

    @Test
    public void loadTestSuite_singleTestWithTestName() {
        String txt = "atomicLongTest@class=AtomicLong" + NEW_LINE
                + "atomicLongTest@threadCount=10";

        TestSuite testSuite = createTestSuite(txt);
        assertEquals(1, testSuite.size());
        TestCase testCase = testSuite.getTestCaseList().get(0);
        assertEquals("atomicLongTest", testCase.getId());
        assertEquals("AtomicLong", testCase.getClassname());
        assertEquals("10", testCase.getProperty("threadCount"));
    }

    @Test
    public void loadTestSuite_multipleCases() {
        String txt = "atomicLongTest@class=AtomicLong" + NEW_LINE
                + "atomicLongTest@threadCount=10" + NEW_LINE
                + "atomicBooleanTest@class=AtomicBoolean" + NEW_LINE
                + "atomicBooleanTest@threadCount=20";

        TestSuite testSuite = createTestSuite(txt);
        assertEquals(2, testSuite.size());

        TestCase atomicLongTestCase = testSuite.getTestCase("atomicLongTest");
        assertEquals("atomicLongTest", atomicLongTestCase.getId());
        assertEquals("AtomicLong", atomicLongTestCase.getClassname());
        assertEquals("10", atomicLongTestCase.getProperty("threadCount"));

        TestCase atomicBooleanTestCase = testSuite.getTestCase("atomicBooleanTest");
        assertEquals("atomicBooleanTest", atomicBooleanTestCase.getId());
        assertEquals("AtomicBoolean", atomicBooleanTestCase.getClassname());
        assertEquals("20", atomicBooleanTestCase.getProperty("threadCount"));
    }

    @Test
    public void loadTestSuite_singleTest() {
        String txt = "class=AtomicLong" + NEW_LINE
                + "threadCount=10";

        TestSuite testSuite = createTestSuite(txt);
        assertEquals(1, testSuite.size());

        TestCase testCase = testSuite.getTestCaseList().get(0);
        assertNotNull(testCase);
        assertEquals("AtomicLong", testCase.getClassname());
        assertNotNull(testCase.toString());

        Map<String, String> properties = testCase.getProperties();
        assertEquals("10", properties.get("threadCount"));
    }

    @Test(expected = BindException.class)
    public void loadTestSuite_missingClassName() {
        String txt = "threadCount=10";

        createTestSuite(txt);
    }

    @Test(expected = BindException.class)
    public void loadTestSuite_missingClassName_withTestCaseId() {
        String txt = "TestCase@threadCount=10";

        createTestSuite(txt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loadTestSuite_emptyClassName() {
        String txt = "class=";

        createTestSuite(txt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loadTestSuite_emptyProperty() {
        String txt = "class=AtomicLong" + NEW_LINE
                + "threadCount=";

        createTestSuite(txt);
    }

    @Test
    public void getTestCase_null() {
        TestSuite testSuite = createTestSuite("");

        assertNull(testSuite.getTestCase(null));
    }

    @Test
    public void getTestCase_notFound() {
        TestSuite testSuite = createTestSuite("");

        assertNull(testSuite.getTestCase("notFound"));
    }

    @Test
    public void getTestCase_toString() {
        TestSuite testSuite = createTestSuite("");

        assertNotNull(testSuite.toString());
    }

    @Test(expected = CommandLineExitException.class)
    public void propertiesNotFound() {
        loadTestSuite(new File("notFound"), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTestCaseId() {
        String txt = "In$valid@class=AtomicLong" + NEW_LINE;

        createTestSuite(txt);
    }

    @Test
    public void overrideProperties() {
        String txt = "class=AtomicLong" + NEW_LINE
                + "threadCount=10";

        String overrideProperties = "threadCount=20";

        TestSuite testSuite = createTestSuite(txt, overrideProperties);
        assertEquals(1, testSuite.size());

        TestCase testCase = testSuite.getTestCaseList().get(0);
        assertEquals("AtomicLong", testCase.getClassname());
        assertEquals("20", testCase.getProperty("threadCount"));
    }

    @Test
    public void testMaxCaseIdLength() {
        TestSuite testSuite = new TestSuite();
        testSuite.addTest(new TestCase("abc"));
        testSuite.addTest(new TestCase("88888888"));
        testSuite.addTest(new TestCase("abcDEF"));
        testSuite.addTest(new TestCase(""));
        testSuite.addTest(new TestCase("four"));

        assertEquals(8, testSuite.getMaxTestCaseIdLength());
    }

    private TestSuite createTestSuite(String txt) {
        return createTestSuite(txt, "");
    }

    private TestSuite createTestSuite(String txt, String overrideProperties) {
        writeText(txt, testSuiteFile);

        return loadTestSuite(testSuiteFile, overrideProperties);
    }
}
