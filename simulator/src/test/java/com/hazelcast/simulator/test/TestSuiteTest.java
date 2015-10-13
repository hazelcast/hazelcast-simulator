package com.hazelcast.simulator.test;

import com.hazelcast.simulator.utils.BindException;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.hazelcast.simulator.test.TestSuite.loadTestSuite;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestSuiteTest {

    @Test
    public void loadTestSuite_singleTestWithTestName() throws Exception {
        String txt = "atomicLongTest@class=AtomicLong\n"
                + "atomicLongTest@threadCount=10";

        TestSuite testSuite = createTestSuite(txt);
        assertEquals(1, testSuite.size());
        TestCase testCase = testSuite.getTestCaseList().get(0);
        assertEquals("atomicLongTest", testCase.getId());
        assertEquals("AtomicLong", testCase.getClassname());
        assertEquals("10", testCase.getProperty("threadCount"));
    }

    @Test
    public void loadTestSuite_multipleCases() throws Exception {
        String txt = "atomicLongTest@class=AtomicLong\n"
                + "atomicLongTest@threadCount=10\n"
                + "atomicBooleanTest@class=AtomicBoolean\n"
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
    public void loadTestSuite_singleTest() throws Exception {
        String txt = "class=AtomicLong\n"
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
    public void loadTestSuite_missingClassName() throws Exception {
        String txt = "threadCount=10";

        createTestSuite(txt);
    }

    @Test(expected = BindException.class)
    public void loadTestSuite_missingClassName_withTestCaseId() throws Exception {
        String txt = "TestCase@threadCount=10";

        createTestSuite(txt);
    }

    @Test
    public void getTestCase_null() throws Exception {
        TestSuite testSuite = createTestSuite("");

        assertNull(testSuite.getTestCase(null));
    }

    @Test
    public void getTestCase_notFound() throws Exception {
        TestSuite testSuite = createTestSuite("");

        assertNull(testSuite.getTestCase("notFound"));
    }

    @Test
    public void getTestCase_toString() throws Exception {
        TestSuite testSuite = createTestSuite("");

        assertNotNull(testSuite.toString());
    }

    @Test(expected = RuntimeException.class)
    public void propertiesNotFound() throws Exception {
        loadTestSuite(new File("notFound"), "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidTestCaseId() throws Exception {
        String txt = "In$valid@class=AtomicLong\n";

        createTestSuite(txt);
    }

    @Test
    public void overrideProperties() throws Exception {
        String txt = "class=AtomicLong\n"
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
        testSuite.addTest(new TestCase(null));
        testSuite.addTest(new TestCase("abcDEF"));
        testSuite.addTest(new TestCase(""));
        testSuite.addTest(new TestCase("four"));

        assertEquals(8, testSuite.getMaxTestCaseIdLength());
    }

    private static TestSuite createTestSuite(String txt) throws Exception {
        return createTestSuite(txt, "");
    }

    private static TestSuite createTestSuite(String txt, String overrideProperties) throws Exception {
        File file = File.createTempFile("simulator", "properties");
        file.deleteOnExit();
        writeText(txt, file);

        return loadTestSuite(file, overrideProperties);
    }
}
