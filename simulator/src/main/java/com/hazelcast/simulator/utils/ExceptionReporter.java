/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.utils.CommonUtils.throwableToString;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.rename;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;

/**
 * Responsible for writing an exception to a file. Every exception file will have a unique name.
 */
public final class ExceptionReporter {

    static final int MAX_EXCEPTION_COUNT = 1000;

    static final AtomicLong FAILURE_ID = new AtomicLong(0);

    private static final Logger LOGGER = Logger.getLogger(ExceptionReporter.class);

    private ExceptionReporter() {
    }

    /**
     * Writes the cause to file.
     *
     * @param testId the id of the test that caused the exception. Is allowed to be <tt>null</tt> if it is not known which test
     *               caused the problem.
     * @param cause  the Throwable that should be reported.
     */
    public static void report(String testId, Throwable cause) {
        if (cause == null) {
            LOGGER.fatal("Can't call report with a null exception");
            return;
        }

        long exceptionCount = FAILURE_ID.incrementAndGet();

        if (exceptionCount > MAX_EXCEPTION_COUNT) {
            LOGGER.warn("Exception #" + exceptionCount + " detected. The maximum number of exceptions has been exceeded, so it"
                    + " won't be reported to the Agent.", cause);
            return;
        }

        LOGGER.warn("Exception #" + exceptionCount + " detected", cause);

        String targetFileName = exceptionCount + ".exception";

        File dir = getUserDir();
        File tmpFile = new File(dir, targetFileName + ".tmp");
        try {
            if (!tmpFile.createNewFile()) {
                throw new IOException("Could not create tmp file: " + tmpFile.getAbsolutePath());
            }
        } catch (IOException e) {
            LOGGER.fatal("Could not report exception; this means that this exception is not visible to the coordinator", e);
            return;
        }

        writeText(testId + NEW_LINE + throwableToString(cause), tmpFile);

        File file = new File(dir, targetFileName);
        LOGGER.info(file.getAbsolutePath());
        rename(tmpFile, file);
    }

    // just for testing
    public static void reset() {
        FAILURE_ID.set(0);
    }
}
