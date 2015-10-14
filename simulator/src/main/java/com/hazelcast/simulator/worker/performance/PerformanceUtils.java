package com.hazelcast.simulator.worker.performance;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FormatUtils.fillString;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

final class PerformanceUtils {

    static final long ONE_SECOND_IN_MILLIS = SECONDS.toMillis(1);

    private static final int NUMBER_FORMAT_LENGTH = 14;

    private static final int HUNDRED = 100;
    private static final int TEN = 10;
    private static final int THREE = 3;

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
        int fieldLength = getNumberOfDigits(totalTests);
        appendText(format(dataString + "\n", timestamp, formatLong(opsSum, NUMBER_FORMAT_LENGTH),
                formatLong(opsDelta, NUMBER_FORMAT_LENGTH), formatDouble(opsPerSecDelta, NUMBER_FORMAT_LENGTH),
                formatLong(numberOfTests, NUMBER_FORMAT_LENGTH - fieldLength), formatLong(totalTests, fieldLength)), file);
    }

    static int getNumberOfDigits(long number) {
        if (number >= HUNDRED) {
            return THREE;
        }
        if (number >= TEN) {
            return 2;
        }
        return 1;
    }
}
