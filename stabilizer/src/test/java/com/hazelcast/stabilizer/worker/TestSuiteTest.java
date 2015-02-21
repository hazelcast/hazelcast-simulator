package com.hazelcast.stabilizer.worker;

import com.hazelcast.stabilizer.test.TestCase;
import com.hazelcast.stabilizer.utils.BindException;
import com.hazelcast.stabilizer.test.TestSuite;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.stabilizer.utils.FileUtils.writeText;
import static org.junit.Assert.assertEquals;

public class TestSuiteTest {

    @Test
    public void loadTestSuite_singleTestWithTestName() throws Exception {
        String txt = "atomiclong@class=AtomicLong\n" +
                "atomiclong@threadcount=10";

        TestSuite suite = load(txt);
        assertEquals(1, suite.testCaseList.size());
        TestCase testCase = suite.testCaseList.get(0);
        assertEquals("atomiclong", testCase.getId());
        assertEquals("AtomicLong", testCase.getClassname());
        assertEquals("10", testCase.getProperty("threadcount"));
    }

    @Test
    public void loadTestSuite_multipleCases() throws Exception {
        String txt = "atomiclong@class=AtomicLong\n"
                + "atomiclong@threadcount=10\n"
                + "atomicboolean@class=AtomicBoolean\n"
                + "atomicboolean@threadcount=20";

        TestSuite suite = load(txt);
        assertEquals(2, suite.testCaseList.size());

        TestCase atomicLongTestCase = suite.getTestCase("atomiclong");
        assertEquals("atomiclong", atomicLongTestCase.getId());
        assertEquals("AtomicLong", atomicLongTestCase.getClassname());
        assertEquals("10", atomicLongTestCase.getProperty("threadcount"));

        TestCase atomicBooleanTestCase = suite.getTestCase("atomicboolean");
        assertEquals("atomicboolean", atomicBooleanTestCase.getId());
        assertEquals("AtomicBoolean", atomicBooleanTestCase.getClassname());
        assertEquals("20", atomicBooleanTestCase.getProperty("threadcount"));
    }

     @Test
    public void loadTestSuite_singleTest() throws Exception {
        String txt = "class=AtomicLong\n" +
                "threadcount=10";

        TestSuite suite = load(txt);
        assertEquals(1, suite.testCaseList.size());

        TestCase testCase = suite.testCaseList.get(0);
        assertEquals("AtomicLong", testCase.getClassname());
        assertEquals("10", testCase.getProperty("threadcount"));
    }

    @Test(expected = BindException.class)
    public void loadTestSuite_missingClassName() throws Exception {
        String txt = "threadcount=10";

        load(txt);
    }

    private TestSuite load(String txt) throws Exception {
        File file = File.createTempFile("stabilizer", "properties");
        file.deleteOnExit();
        writeText(txt, file);

        return TestSuite.loadTestSuite(file, "");
    }
}
