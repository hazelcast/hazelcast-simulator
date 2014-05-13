package com.hazelcast.stabilizer.worker;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.Utils.throwableToString;
import static com.hazelcast.stabilizer.Utils.writeText;

public class ExceptionReporter {

    private final static AtomicLong FAILURE_ID = new AtomicLong(1);
    private final static ILogger log = Logger.getLogger(ExceptionReporter.class);

    public static void report(Throwable cause) {
        log.severe("Exception detected", cause);
        sleepSeconds(2);

        //we need to write to a temp file before and then rename the file so that the worker will not see
        //a partially written failure.
        final File tmpFile;
        try {
            tmpFile = File.createTempFile("worker", "exception");
        } catch (IOException e) {
            log.severe("Failed to create temp file", e);
            return;
        }

        writeText(throwableToString(cause), tmpFile);

        final File file = new File(FAILURE_ID.incrementAndGet() + ".exception");
        if (!tmpFile.renameTo(file)) {
            log.severe("Failed to rename tmp file:" + tmpFile + " to " + file);
        }
    }

    private ExceptionReporter() {
    }
}
