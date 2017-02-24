package com.hazelcast.simulator.common;

import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
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
    public void loadTestSuite_whenInvalidTestId() throws Exception {
        String txt = "*@class=AtomicLong" + NEW_LINE;

        new TestSuite(txt);
    }

    @Test
    public void loadTestSuite_singleTestWithTestName() throws Exception {
        String txt = "atomicLongTest@class=AtomicLong" + NEW_LINE
                + "atomicLongTest@threadCount=10";

        TestSuite testSuite = new TestSuite(txt);
        assertEquals(1, testSuite.size());
        TestCase testCase = testSuite.getTestCaseList().get(0);
        assertEquals("atomicLongTest", testCase.getId());
        assertEquals("AtomicLong", testCase.getClassname());
        assertEquals("10", testCase.getProperty("threadCount"));
    }

    @Test
    public void loadTestSuite_multipleCases() throws Exception {
        String txt = "atomicLongTest@class=AtomicLong" + NEW_LINE
                + "atomicLongTest@threadCount=10" + NEW_LINE
                + "atomicBooleanTest@class=AtomicBoolean" + NEW_LINE
                + "atomicBooleanTest@threadCount=20";

        TestSuite testSuite = new TestSuite(txt);
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
    public void loadTestSuite_singleTest() throws Exception {
        String txt = "class=AtomicLong" + NEW_LINE
                + "threadCount=10";

        TestSuite testSuite = new TestSuite(txt);
        assertEquals(1, testSuite.size());

        TestCase testCase = testSuite.getTestCaseList().get(0);
        assertNotNull(testCase);
        assertEquals("AtomicLong", testCase.getClassname());
        assertNotNull(testCase.toString());

        Map<String, String> properties = testCase.getProperties();
        assertEquals("10", properties.get("threadCount"));
    }

    @Test(expected = BindException.class)
    public void loadTestSuite_missingClassName() throws Exception {
        String txt = "threadCount=10";

        new TestSuite(txt);
    }

    @Test(expected = BindException.class)
    public void loadTestSuite_missingClassName_withTestCaseId() throws Exception {
        String txt = "TestCase@threadCount=10";

        new TestSuite(txt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loadTestSuite_emptyClassName() throws Exception {
        String txt = "class=";

        new TestSuite(txt);
    }

    @Test(expected = IllegalArgumentException.class)
    public void loadTestSuite_emptyProperty() throws Exception {
        String txt = "class=AtomicLong" + NEW_LINE
                + "threadCount=";

        new TestSuite(txt);
    }

    @Test
    public void getTestCase_null() throws Exception {
        TestSuite testSuite = new TestSuite("");

        assertNull(testSuite.getTestCase(null));
    }

    @Test
    public void getTestCase_notFound() throws Exception {
        TestSuite testSuite = new TestSuite("");

        assertNull(testSuite.getTestCase("notFound"));
    }

    @Test
    public void getTestCase_toString() throws Exception {
        TestSuite testSuite = new TestSuite("");

        assertNotNull(testSuite.toString());
    }

    @Test(expected = CommandLineExitException.class)
    public void propertiesNotFound() throws Exception {
        new TestSuite(new File("notFound"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTestCaseId() throws Exception {
        String txt = "In$valid@class=AtomicLong" + NEW_LINE;

        new TestSuite(txt);
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
}
