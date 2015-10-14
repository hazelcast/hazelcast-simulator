package com.hazelcast.simulator.utils;

import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Locale;

import static java.lang.String.format;

public final class FormatUtils {

    public static final String NEW_LINE = System.getProperty("line.separator");

    private static final double ONE_HUNDRED = 100;
    private static final int PERCENTAGE_FORMAT_LENGTH = 7;
    private static final String DEFAULT_DELIMITER = ", ";

    private static final int SECONDS_PER_MINUTE = 60;
    private static final int MINUTES_PER_HOUR = 60;
    private static final int HOURS_PER_DAY = 24;

    private static final int SI_BYTES_FACTOR = 1000;
    private static final int IEC_BYTES_FACTOR = 1024;

    private FormatUtils() {
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
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb, Locale.US);
        formatter.format("%,.2f", number);

        return padLeft(sb.toString(), length);
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
        StringBuilder sb = new StringBuilder();
        Formatter formatter = new Formatter(sb, Locale.US);
        formatter.format("%,d", number);

        return padLeft(sb.toString(), length);
    }

    public static String padRight(String argument, int length) {
        if (length <= 0) {
            return argument;
        }
        return String.format("%-" + length + "s", argument);
    }

    public static String padLeft(String argument, int length) {
        if (length <= 0) {
            return argument;
        }
        return String.format("%" + length + "s", argument);
    }

    public static String fillString(int length, char charToFill) {
        if (length == 0) {
            return "";
        }
        char[] array = new char[length];
        Arrays.fill(array, charToFill);
        return new String(array);
    }

    public static String secondsToHuman(long seconds) {
        long time = seconds;

        long s = time % SECONDS_PER_MINUTE;
        time /= SECONDS_PER_MINUTE;

        long m = time % MINUTES_PER_HOUR;
        time /= MINUTES_PER_HOUR;

        long h = time % HOURS_PER_DAY;
        time /= HOURS_PER_DAY;

        long days = time;

        return format("%02dd %02dh %02dm %02ds", days, h, m, s);
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
            Object o = iterator.next();
            builder.append(o);
            if (iterator.hasNext()) {
                builder.append(delimiter);
            }
        }
        return builder.toString();
    }
}
