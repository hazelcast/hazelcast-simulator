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
package com.hazelcast.simulator.utils;

import java.util.Iterator;
import java.util.Locale;

import static java.lang.String.format;
import static java.util.Arrays.fill;

public final class FormatUtils {

    public static final String NEW_LINE = System.getProperty("line.separator");
    public static final String HORIZONTAL_RULER = "=========================================================================";

    private static final double ONE_HUNDRED = 100;
    private static final int IP_ADDRESS_LENGTH = 15;
    private static final int PERCENTAGE_FORMAT_LENGTH = 6;
    private static final String DEFAULT_DELIMITER = ", ";

    private static final int SECONDS_PER_MINUTE = 60;
    private static final int MINUTES_PER_HOUR = 60;
    private static final int HOURS_PER_DAY = 24;

    private static final int SI_BYTES_FACTOR = 1000;
    private static final int IEC_BYTES_FACTOR = 1024;

    private FormatUtils() {
    }

    /**
     * Formats an IP address by adding padding to the left.
     *
     * @param ipAddress the IP address
     * @return the formatted IP address
     */
    public static String formatIpAddress(String ipAddress) {
        return padLeft(ipAddress, IP_ADDRESS_LENGTH);
    }

    /**
     * Formats a percentage of two numbers and adds padding to the left.
     *
     * @param value     the value of the percentage
     * @param baseValue the base value of the percentage
     * @return the formatted percentage
     */
    public static String formatPercentage(long value, long baseValue) {
        double percentage = (baseValue > 0 ? (ONE_HUNDRED * value) / baseValue : 0);
        return formatDouble(percentage, PERCENTAGE_FORMAT_LENGTH);
    }

    /**
     * Formats a double number and adds padding to the left.
     *
     * Very inefficient implementation, but a lot easier than to deal with the formatting API.
     *
     * @param number number to format
     * @param length width of padding
     * @return formatted number
     */
    public static String formatDouble(double number, int length) {
        return padLeft(format(Locale.US, "%,.2f", number), length);
    }

    /**
     * Formats a long number and adds padding to the left.
     *
     * Very inefficient implementation, but a lot easier than to deal with the formatting API.
     *
     * @param number number to format
     * @param length width of padding
     * @return formatted number
     */
    public static String formatLong(long number, int length) {
        return padLeft(format(Locale.US, "%,d", number), length);
    }

    public static String padRight(String argument, int length) {
        if (length <= 0) {
            return argument;
        }
        return format("%-" + length + "s", argument);
    }

    public static String padLeft(String argument, int length) {
        if (length <= 0) {
            return argument;
        }
        return format("%" + length + "s", argument);
    }

    public static String fillString(int length, char charToFill) {
        if (length == 0) {
            return "";
        }
        char[] array = new char[length];
        fill(array, charToFill);
        return new String(array);
    }

    public static String secondsToHuman(long seconds) {
        long time = seconds;

        long moduloSeconds = time % SECONDS_PER_MINUTE;
        time /= SECONDS_PER_MINUTE;

        long minutes = time % MINUTES_PER_HOUR;
        time /= MINUTES_PER_HOUR;

        long hours = time % HOURS_PER_DAY;
        time /= HOURS_PER_DAY;

        long days = time;

        return format("%02dd %02dh %02dm %02ds", days, hours, minutes, moduloSeconds);
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? SI_BYTES_FACTOR : IEC_BYTES_FACTOR;
        if (bytes < unit) {
            return bytes + " B";
        }
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return format(Locale.US, "%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static String join(Iterable<?> collection) {
        return join(collection, DEFAULT_DELIMITER);
    }

    public static String join(Iterable<?> collection, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<?> iterator = collection.iterator();
        while (iterator.hasNext()) {
            Object entry = iterator.next();
            builder.append(entry);
            if (iterator.hasNext()) {
                builder.append(delimiter);
            }
        }
        return builder.toString();
    }
}
