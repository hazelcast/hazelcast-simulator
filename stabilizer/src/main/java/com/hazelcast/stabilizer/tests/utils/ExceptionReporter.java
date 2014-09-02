package com.hazelcast.stabilizer.tests.utils;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.throwableToString;
import static com.hazelcast.stabilizer.Utils.writeText;

/**
 * Responsible for writing an Exception to file. Every exception-file will have a unique name.
 */
public class ExceptionReporter {

    private final static AtomicLong FAILURE_ID = new AtomicLong(1);
    private final static ILogger log = Logger.getLogger(ExceptionReporter.class);

    /**
     * Writes the cause to file.
     *
     * @param testId the id of the test that caused the exception. Is allowed to be null if it is not known
     *               which test caused the problem.
     * @param cause  the Throwable that should be reported.
     */
    public static void report(String testId, Throwable cause) {
        if (cause == null) {
            log.severe("Can't call report with a null exception");
            return;
        }

        log.severe("Exception detected", cause);

        final File tmpFile = createTmpFile();
        if (tmpFile == null) {
            return;
        }

        writeCauseToFile(testId, cause, tmpFile);

        final File file = new File(FAILURE_ID.incrementAndGet() + ".exception");
        if (!tmpFile.renameTo(file)) {
            log.severe("Failed to rename tmp file:" + tmpFile + " to " + file);
        }
    }

    private static void writeCauseToFile(String testId, Throwable cause, File file) {
        String text = testId + "\n" + throwableToString(cause);
        writeText(text, file);
    }

    private static File createTmpFile() {
        //we need to write to a temp file before and then rename the file so that the worker will not see
        //a partially written failure.
        final File tmpFile;
        try {
            tmpFile = File.createTempFile("worker", "exception");
        } catch (IOException e) {
            log.severe("Failed to create temp file", e);
            return null;
        }
        return tmpFile;
    }

    private ExceptionReporter() {
    }
}
