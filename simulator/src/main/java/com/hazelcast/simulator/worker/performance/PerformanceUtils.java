/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker.performance;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FormatUtils.fillString;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility class for performance related methods.
 */
final class PerformanceUtils {

    static final long ONE_SECOND_IN_MILLIS = SECONDS.toMillis(1);

    private static final int NUMBER_FORMAT_LENGTH = 14;

    private static final int HUNDRED = 100;
    private static final int TEN = 10;
    private static final int THREE = 3;
    private static final int TWO = 2;
    private static final int ONE = 1;

    private PerformanceUtils() {
    }

    static void writeThroughputHeader(File file, boolean isGlobal) {
        String columns = "Timestamp                      Ops (sum)        Ops (delta)                Ops/s";
        if (isGlobal) {
            columns += " Number of tests";
        }
        appendText(format("%s%n%s%n", columns, fillString(columns.length(), '-')), file);
    }

    static void writeThroughputStats(File file, String timestamp, long opsSum, long opsDelta, double opsPerSecDelta,
                                     long numberOfTests, long totalTests) {
        String dataString = "[%s] %s ops %s ops %s ops/s";
        if (totalTests > 0) {
            dataString += " %s/%s";
        }
        dataString += "%n";
        int fieldLength = getNumberOfDigits(totalTests);
        appendText(format(dataString, timestamp,
                formatLong(opsSum, NUMBER_FORMAT_LENGTH),
                formatLong(opsDelta, NUMBER_FORMAT_LENGTH),
                formatDouble(opsPerSecDelta, NUMBER_FORMAT_LENGTH),
                formatLong(numberOfTests, NUMBER_FORMAT_LENGTH - fieldLength),
                formatLong(totalTests, fieldLength)),
                file);
    }

    static int getNumberOfDigits(long number) {
        if (number >= HUNDRED) {
            return THREE;
        }
        if (number >= TEN) {
            return TWO;
        }
        return ONE;
    }
}
