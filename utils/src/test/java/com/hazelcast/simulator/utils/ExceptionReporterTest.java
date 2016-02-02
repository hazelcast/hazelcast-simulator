package com.hazelcast.simulator.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.ExceptionReporter.report;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ExceptionReporterTest {

    private static final File EXCEPTION_FILE = new File("1.exception");
    private static final File TMP_EXCEPTION_FILE = new File("1.exception.tmp");

    @Before
    public void setUp() {
        deleteQuiet(EXCEPTION_FILE);
        ExceptionReporter.reset();
    }

    @After
    public void tearDown() {
        deleteQuiet(EXCEPTION_FILE);
        ExceptionReporter.reset();
    }

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(ExceptionReporter.class);
    }

    @Test
    public void testReportNullCause() {
        report("testID", null);

        assertFalse(EXCEPTION_FILE.exists());
    }

    @Test
    public void testReport() {
        report("testID", new RuntimeException("Expected exception"));

        assertTrue(EXCEPTION_FILE.exists());
        assertNotNull(fileAsText(EXCEPTION_FILE));
    }

    @Test
    public void testReportTooManyExceptions() {
        ExceptionReporter.FAILURE_ID.set(ExceptionReporter.MAX_EXCEPTION_COUNT + 1);
        report("testID", new RuntimeException("Expected exception"));

        assertFalse(EXCEPTION_FILE.exists());
    }

    @Test
    public void testReportTmpFileExists() {
        ensureExistingFile(TMP_EXCEPTION_FILE);
        report("testID", new RuntimeException("Expected exception"));

        assertTrue(TMP_EXCEPTION_FILE.exists());
        assertFalse(EXCEPTION_FILE.exists());

        deleteQuiet(TMP_EXCEPTION_FILE);
    }
}
