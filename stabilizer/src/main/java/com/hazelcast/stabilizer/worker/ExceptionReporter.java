package com.hazelcast.stabilizer.worker;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.sleepSeconds;
import static com.hazelcast.stabilizer.Utils.writeText;

public class ExceptionReporter {

    private final static AtomicLong FAILURE_ID = new AtomicLong(1);
    private final static ILogger log = Logger.getLogger(ExceptionReporter.class);

    public static void report(Throwable t) {
        System.setProperty("workerId", UUID.randomUUID().toString());

        log.severe("Exception detected", t);
        sleepSeconds(2);

        //we need to write to a temp file before and then rename the file so that the worker will not see
        //a partially written failure.
        final File tmpFile;
        try {
            tmpFile = File.createTempFile("worker", "exception");
        } catch (IOException e) {
            log.severe("Failed to create temp file", e);
            throw new RuntimeException(e);
        }

        try {
            writeText(Utils.throwableToString(t), tmpFile);
        } catch (IOException e) {
            log.severe("Failed to write to tmpFile:" + tmpFile, e);
            throw new RuntimeException(e);
        }

        final File file = new File(getWorkerId() + "@" + FAILURE_ID.incrementAndGet() + ".failure");
        if (!tmpFile.renameTo(file)) {
            throw new RuntimeException("Failed to rename tmp file:" + tmpFile + " to " + file);
        }
    }

    public static String getWorkerId() {
        return System.getProperty("workerId");
    }

    private ExceptionReporter() {
    }
}
