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

import org.junit.Test;

import java.util.Collections;

import static com.hazelcast.simulator.utils.FormatUtils.fillString;
import static com.hazelcast.simulator.utils.FormatUtils.formatDouble;
import static com.hazelcast.simulator.utils.FormatUtils.formatIpAddress;
import static com.hazelcast.simulator.utils.FormatUtils.formatLong;
import static com.hazelcast.simulator.utils.FormatUtils.formatPercentage;
import static com.hazelcast.simulator.utils.FormatUtils.humanReadableByteCount;
import static com.hazelcast.simulator.utils.FormatUtils.join;
import static com.hazelcast.simulator.utils.FormatUtils.padLeft;
import static com.hazelcast.simulator.utils.FormatUtils.padRight;
import static com.hazelcast.simulator.utils.FormatUtils.secondsToHuman;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FormatUtilsTest {

    @Test
    public void testConstructor() throws Exception {
        invokePrivateConstructor(FormatUtils.class);
    }

    @Test
    public void testFormatIpAddress() {
        assertTrue(formatIpAddress("127.0.0.1").contains("127.0.0.1"));
        assertTrue(formatIpAddress("10.0.0.7").contains("10.0.0.7"));
        assertTrue(formatIpAddress("192.168.0.1").contains("192.168.0.1"));
        assertTrue(formatIpAddress("172.16.16.137").contains("172.16.16.137"));
    }

    @Test
    public void testFormatPercentage() {
        assertEquals("  0.00", formatPercentage(0, 10));
        assertEquals(" 30.00", formatPercentage(3, 10));
        assertEquals(" 50.00", formatPercentage(5, 10));
        assertEquals("100.00", formatPercentage(10, 10));
        assertEquals(" 49.18", formatPercentage(30, 61));
    }

    @Test
    public void testFormatLong() {
        assertEquals("-1", formatLong(-1, -10));
        assertEquals("-1", formatLong(-1, 0));
        assertEquals("        -1", formatLong(-1, 10));

        assertEquals("0", formatLong(0, -10));
        assertEquals("0", formatLong(0, 0));
        assertEquals("         0", formatLong(0, 10));

        assertEquals("1", formatLong(1, -10));
        assertEquals("1", formatLong(1, 0));
        assertEquals("         1", formatLong(1, 10));

        assertEquals("-9,223,372,036,854,775,808", formatLong(Long.MIN_VALUE, -30));
        assertEquals("-9,223,372,036,854,775,808", formatLong(Long.MIN_VALUE, 0));
        assertEquals("    -9,223,372,036,854,775,808", formatLong(Long.MIN_VALUE, 30));

        assertEquals("9,223,372,036,854,775,807", formatLong(Long.MAX_VALUE, -30));
        assertEquals("9,223,372,036,854,775,807", formatLong(Long.MAX_VALUE, 0));
        assertEquals("     9,223,372,036,854,775,807", formatLong(Long.MAX_VALUE, 30));
    }

    @Test
    public void testFormatDouble() {
        assertEquals("-1.00", formatDouble(-1.0d, -10));
        assertEquals("-1.00", formatDouble(-1.0d, 0));
        assertEquals("     -1.00", formatDouble(-1.0d, 10));

        assertEquals("0.00", formatDouble(0.0d, -10));
        assertEquals("0.00", formatDouble(0.0d, 0));
        assertEquals("      0.00", formatDouble(0.0d, 10));

        assertEquals("1.00", formatDouble(1.0d, -10));
        assertEquals("1.00", formatDouble(1.0d, 0));
        assertEquals("      1.00", formatDouble(1.0d, 10));

        assertEquals("1.50", formatDouble(1.5d, -10));
        assertEquals("1.50", formatDouble(1.5d, 0));
        assertEquals("      1.50", formatDouble(1.5d, 10));

        assertEquals("1.51", formatDouble(1.505d, -10));
        assertEquals("1.51", formatDouble(1.505d, 0));
        assertEquals("      1.51", formatDouble(1.505d, 10));
    }

    @Test
    public void testPadRight() {
        assertEquals(null, padRight(null, -10));
        assertEquals(null, padRight(null, 0));
        assertEquals("null      ", padRight(null, 10));

        assertEquals("", padRight("", -10));
        assertEquals("", padRight("", 0));
        assertEquals("          ", padRight("", 10));

        assertEquals("test", padRight("test", -10));
        assertEquals("test", padRight("test", 0));
        assertEquals("test      ", padRight("test", 10));

        assertEquals("longerString", padRight("longerString", -5));
        assertEquals("longerString", padRight("longerString", 0));
        assertEquals("longerString", padRight("longerString", 5));
    }

    @Test
    public void testPadLeft() {
        assertEquals(null, padLeft(null, -10));
        assertEquals(null, padLeft(null, 0));
        assertEquals("      null", padLeft(null, 10));

        assertEquals("", padLeft("", -10));
        assertEquals("", padLeft("", 0));
        assertEquals("          ", padLeft("", 10));

        assertEquals("test", padLeft("test", -10));
        assertEquals("test", padLeft("test", 0));
        assertEquals("      test", padLeft("test", 10));

        assertEquals("longerString", padLeft("longerString", -5));
        assertEquals("longerString", padLeft("longerString", 0));
        assertEquals("longerString", padLeft("longerString", 5));
    }

    @Test
    public void testFillStringZeroLength() {
        String actual = fillString(0, '#');
        assertTrue(format("Expected empty string, but got %s", actual), actual.isEmpty());
    }

    @Test
    public void testFillString() {
        String actual = fillString(5, '#');
        assertEqualsStringFormat("Expected filled string %s, but was %s", "#####", actual);
    }

    @Test
    public void testSecondsToHuman() {
        String expected = "01d 02h 03m 04s";
        String actual = secondsToHuman(93784);
        assertEqualsStringFormat("Expected human readable seconds to be %s, but was %s", expected, actual);
    }

    @Test
    public void testHumanReadableByteCount_Byte_SI() {
        String actual = humanReadableByteCount(42, false);
        assertEqualsStringFormat("Expected %s, but was %s", "42 B", actual);
    }

    @Test
    public void testHumanReadableByteCount_Byte_NoSI() {
        String actual = humanReadableByteCount(23, true);
        assertEqualsStringFormat("Expected %s, but was %s", "23 B", actual);
    }

    @Test
    public void testHumanReadableByteCount_KiloByte_SI() {
        String actual = humanReadableByteCount(4200, false);
        assertEqualsStringFormat("Expected %s, but was %s", "4.1 KiB", actual);
    }

    @Test
    public void testHumanReadableByteCount_KiloByte_NoSI() {
        String actual = humanReadableByteCount(2300, true);
        assertEqualsStringFormat("Expected %s, but was %s", "2.3 kB", actual);
    }

    @Test
    public void testHumanReadableByteCount_MegaByte_SI() {
        String actual = humanReadableByteCount(4200000, false);
        assertEqualsStringFormat("Expected %s, but was %s", "4.0 MiB", actual);
    }

    @Test
    public void testHumanReadableByteCount_MegaByte_NoSI() {
        String actual = humanReadableByteCount(2300000, true);
        assertEqualsStringFormat("Expected %s, but was %s", "2.3 MB", actual);
    }

    @Test
    public void testHumanReadableByteCount_GigaByte_SI() {
        String actual = humanReadableByteCount(Integer.MAX_VALUE, false);
        assertEqualsStringFormat("Expected %s, but was %s", "2.0 GiB", actual);
    }

    @Test
    public void testHumanReadableByteCount_GigaByte_NoSI() {
        String actual = humanReadableByteCount(Integer.MAX_VALUE, true);
        assertEqualsStringFormat("Expected %s, but was %s", "2.1 GB", actual);
    }

    @Test
    public void testJoin() {
        Iterable<String> input = asList("one", "two", "three");
        String joined = join(input);
        assertEquals("one, two, three", joined);
    }

    @Test
    public void testJoinEmptyString() {
        String joined = join(Collections.EMPTY_LIST);
        assertEquals("", joined);
    }
}
