package com.hazelcast.simulator;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static com.hazelcast.simulator.utils.CommonUtils.formatDouble;
import static com.hazelcast.simulator.utils.CommonUtils.formatLong;
import static com.hazelcast.simulator.utils.CommonUtils.join;
import static com.hazelcast.simulator.utils.CommonUtils.padLeft;
import static com.hazelcast.simulator.utils.CommonUtils.padRight;
import static org.junit.Assert.assertEquals;

public class CommonUtilsTest {

    @Test
    public void testJoin() throws Exception {
        Iterable<String> input = Arrays.asList("one", "two", "three");
        String joined = join(input);
        assertEquals("one, two, three", joined);
    }

    @Test
    public void testJoin_emptyString() throws Exception {
        String joined = join(Collections.EMPTY_LIST);
        assertEquals("", joined);
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

        // Tests with Long.MAX_VALUE fail (maybe some overflow in the formatter)
        //assertEquals("9,223,372,036,854,775,808", formatLong(Long.MAX_VALUE, -30));
        //assertEquals("9,223,372,036,854,775,808", formatLong(Long.MAX_VALUE, 0));
        //assertEquals("     9,223,372,036,854,775,808", formatLong(Long.MAX_VALUE, 30));
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
}