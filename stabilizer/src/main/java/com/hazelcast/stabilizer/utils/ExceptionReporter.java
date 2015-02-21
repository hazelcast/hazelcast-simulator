package com.hazelcast.stabilizer.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.utils.CommonUtils.throwableToString;
import static com.hazelcast.stabilizer.utils.FileUtils.writeText;

/**
 * Responsible for writing an Exception to file. Every exception-file will have a unique name.
 */
public class ExceptionReporter {

    public static final int MAX_EXCEPTION_COUNT = 1000;

    private final static AtomicLong FAILURE_ID = new AtomicLong(0);
    private final static Logger log = Logger.getLogger(ExceptionReporter.class);

    /**
     * Writes the cause to file.
     *
     * @param testId the id of the test that caused the exception. Is allowed to be null if it is not known
     *               which test caused the problem.
     * @param cause  the Throwable that should be reported.
     */
    public static void report(String testId, Throwable cause) {
        if (cause == null) {
            log.fatal("Can't call report with a null exception");
            return;
        }

        long exceptionCount = FAILURE_ID.incrementAndGet();

        if (exceptionCount > MAX_EXCEPTION_COUNT) {
            log.warn("Exception #" + exceptionCount + " detected. The maximum number of exceptions has been" +
                    "exceeded, so it won't be reported to the agent.", cause);
            return;
        }

        log.warn("Exception #" + exceptionCount + " detected", cause);

        String targetFileName = exceptionCount + ".exception";

        final File tmpFile = new File(targetFileName + ".tmp");

        try {
            if (!tmpFile.createNewFile()) {
                // can't happen since id's are always incrementing. So just for safety reason this is added.
                throw new IOException("Could not create tmp file:" + tmpFile.getAbsolutePath() + " file already exists.");
            }
        } catch (IOException e) {
            log.fatal("Could not report exception; this means that this exception is not visible to the coordinator", e);
            return;
        }

        writeCauseToFile(testId, cause, tmpFile);

        final File file = new File(targetFileName);

        if (!tmpFile.renameTo(file)) {
            log.fatal("Failed to rename tmp file:" + tmpFile + " to " + file);
        }
    }

    private static void writeCauseToFile(String testId, Throwable cause, File file) {
        String text = testId + "\n" + throwableToString(cause);
        writeText(text, file);
    }

    private ExceptionReporter() {
    }
}
