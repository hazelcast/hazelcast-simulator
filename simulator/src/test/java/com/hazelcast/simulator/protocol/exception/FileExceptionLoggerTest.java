package com.hazelcast.simulator.protocol.exception;

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FileExceptionLoggerTest {

    private static final File EXCEPTION_FILE = new File("1.exception");
    private static final File TMP_EXCEPTION_FILE = new File("1.exception.tmp");

    private FileExceptionLogger exceptionLogger;

    @Before
    public void setUp() {
        deleteQuiet(EXCEPTION_FILE);
        deleteQuiet(TMP_EXCEPTION_FILE);

        exceptionLogger = new FileExceptionLogger(SimulatorAddress.COORDINATOR, ExceptionType.WORKER_EXCEPTION);
    }

    @After
    public void tearDown() {
        deleteQuiet(EXCEPTION_FILE);
        deleteQuiet(TMP_EXCEPTION_FILE);
    }

    @Test
    public void testLog() {
        exceptionLogger.log(new RuntimeException("Expected exception"));

        assertTrue(EXCEPTION_FILE.exists());
        assertNotNull(fileAsText(EXCEPTION_FILE));
    }

    @Test
    public void testLog_tmpFileExists() {
        ensureExistingFile(TMP_EXCEPTION_FILE);
        exceptionLogger.log(new RuntimeException("Expected exception"));

        assertTrue(TMP_EXCEPTION_FILE.exists());
        assertFalse(EXCEPTION_FILE.exists());
    }
}
