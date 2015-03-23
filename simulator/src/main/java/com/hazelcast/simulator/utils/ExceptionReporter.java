package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FileUtils.writeText;

/**
 * Responsible for writing an Exception to file. Every exception-file will have a unique name.
 */
public final class ExceptionReporter {

    public static final int MAX_EXCEPTION_COUNT = 1000;

    private static final AtomicLong FAILURE_ID = new AtomicLong(0);
    private static final Logger LOGGER = Logger.getLogger(ExceptionReporter.class);

    private ExceptionReporter() {
    }

    /**
     * Writes the cause to file.
     *
     * @param testId the id of the test that caused the exception. Is allowed to be null if it is not known
     *               which test caused the problem.
     * @param cause  the Throwable that should be reported.
     */
    public static void report(String testId, Throwable cause) {
        if (cause == null) {
            LOGGER.fatal("Can't call report with a null exception");
            return;
        }

        long exceptionCount = FAILURE_ID.incrementAndGet();

        if (exceptionCount > MAX_EXCEPTION_COUNT) {
            LOGGER.warn("Exception #" + exceptionCount + " detected. The maximum number of exceptions has been"
                    + "exceeded, so it won't be reported to the agent.", cause);
            return;
        }

        LOGGER.warn("Exception #" + exceptionCount + " detected", cause);

        String targetFileName = exceptionCount + ".exception";

        final File tmpFile = new File(targetFileName + ".tmp");

        try {
            if (!tmpFile.createNewFile()) {
                // Should not happen since IDs are always incrementing (so this is just for safety reasons)
                throw new IOException("Could not create tmp file:" + tmpFile.getAbsolutePath() + " file already exists.");
            }
        } catch (IOException e) {
            LOGGER.fatal("Could not report exception; this means that this exception is not visible to the coordinator", e);
            return;
        }

        writeCauseToFile(testId, cause, tmpFile);

        final File file = new File(targetFileName);

        if (!tmpFile.renameTo(file)) {
            LOGGER.fatal("Failed to rename tmp file:" + tmpFile + " to " + file);
        }
    }

    private static void writeCauseToFile(String testId, Throwable cause, File file) {
        String text = testId + "\n" + throwableToString(cause);
        writeText(text, file);
    }

}
