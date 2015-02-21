/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.utils;

import org.apache.log4j.Logger;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

public final class CommonUtils {
    public static final String NEW_LINE = System.getProperty("line.separator");

    private static final String DEFAULT_DELIMITER = ", ";
    private static final String EXCEPTION_SEPARATOR = "------ End remote and begin local stack-trace ------";

    private static volatile String hostAddress;

    private CommonUtils() {
    }

    /**
     * Formats a number and adds padding to the left.
     * It is very inefficient; but a lot easier to deal with the formatting API.
     *
     * @param number number to format
     * @param length width of padding
     * @return formatted number
     */
    public static String formatDouble(double number, int length) {
        StringBuffer sb = new StringBuffer();
        Formatter f = new Formatter(sb);
        f.format("%,.2f", number);

        return padLeft(sb.toString(), length);
    }

    public static String formatLong(long number, int length) {
        StringBuffer sb = new StringBuffer();
        Formatter f = new Formatter(sb);
        f.format("%,d", number);

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

        long s = time % 60;

        time = time / 60;
        long m = time % 60;

        time = time / 60;
        long h = time % 24;

        time = time / 24;
        long days = time;

        StringBuilder sb = new StringBuilder();
        sb.append(format("%02d", days)).append("d ").append(format("%02d", h)).append("h ").append(format("%02d", m)).append("m ")
          .append(format("%02d", s)).append("s");

        return sb.toString();
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

    public static String getHostAddress() {
        if (hostAddress != null) {
            return hostAddress;
        }

        synchronized (CommonUtils.class) {
            try {
                if (hostAddress != null) {
                    return hostAddress;
                }
                Socket s = new Socket("google.com", 80);
                hostAddress = s.getLocalAddress().getHostAddress();
                s.close();
                return hostAddress;
            } catch (IOException io) {
                throw new RuntimeException(io);
            }
        }
    }

    public static String getVersion() {
        return CommonUtils.class.getPackage().getImplementationVersion();
    }

    public static void fixRemoteStackTrace(Throwable remoteCause, StackTraceElement[] localSideStackTrace) {
        StackTraceElement[] remoteStackTrace = remoteCause.getStackTrace();
        StackTraceElement[] newStackTrace = new StackTraceElement[localSideStackTrace.length + remoteStackTrace.length];
        System.arraycopy(remoteStackTrace, 0, newStackTrace, 0, remoteStackTrace.length);
        newStackTrace[remoteStackTrace.length] = new StackTraceElement(EXCEPTION_SEPARATOR, "", null, -1);
        System.arraycopy(localSideStackTrace, 1, newStackTrace, remoteStackTrace.length + 1, localSideStackTrace.length - 1);
        remoteCause.setStackTrace(newStackTrace);
    }

    public static String throwableToString(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }

        try {
            socket.close();
        } catch (IOException ignore) {
        }
    }

    public static void closeQuietly(Closeable... closeables) {
        for (Closeable c : closeables) {
            closeQuietly(c);
        }
    }

    public static void closeQuietly(Closeable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException ignore) {
        }
    }

    public static void closeQuietly(XMLStreamWriter c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (XMLStreamException ignore) {
        }
    }

    public static void sleepSeconds(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException ignored) {
        }
    }

    public static void sleepMillis(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    public static void sleepNanos(long nanos) {
        if (nanos <= 0) {
            return;
        }

        LockSupport.parkNanos(nanos);
    }

    public static void sleepSecondsThrowException(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sleepMillisThrowException(int millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sleeps a random amount of time.
     * <p/>
     * The call is ignored if maxDelayNanos equals or smaller than zero.
     *
     * @param random        the Random used to randomize
     * @param maxDelayNanos the maximum sleeping period in nanoseconds
     */
    public static void sleepRandomNanos(Random random, long maxDelayNanos) {
        if (maxDelayNanos <= 0) {
            return;
        }

        long randomValue = Math.abs(random.nextLong() + 1);
        sleepNanos(randomValue % maxDelayNanos);
    }

    public static void exitWithError(Logger logger, String msg) {
        logger.fatal(msg);
        System.exit(1);
    }

    public static void exitWithError(Logger logger, String msg, Throwable t) {
        String throwableString = throwableToString(t);
        exitWithError(logger, msg + "\n" + throwableString);
    }
}
