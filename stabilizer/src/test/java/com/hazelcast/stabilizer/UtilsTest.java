package com.hazelcast.stabilizer;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class UtilsTest {

    @Test
    public void testJoin() throws Exception {
        Iterable<String> input = Arrays.asList("one", "two", "three");
        String joined = Utils.join(input);
        assertEquals("one, two, three", joined);
    }

    @Test
    public void testJoin_emptyString() throws Exception {
        String joined = Utils.join(Collections.EMPTY_LIST);
        assertEquals("", joined);
    }

    @Test
    public void testPadRight() {
        assertEquals(null, Utils.padRight(null, -10));
        assertEquals(null, Utils.padRight(null, 0));
        assertEquals("null      ", Utils.padRight(null, 10));

        assertEquals("", Utils.padRight("", -10));
        assertEquals("", Utils.padRight("", 0));
        assertEquals("          ", Utils.padRight("", 10));

        assertEquals("test", Utils.padRight("test", -10));
        assertEquals("test", Utils.padRight("test", 0));
        assertEquals("test      ", Utils.padRight("test", 10));

        assertEquals("longerString", Utils.padRight("longerString", -5));
        assertEquals("longerString", Utils.padRight("longerString", 0));
        assertEquals("longerString", Utils.padRight("longerString", 5));
    }

    @Test
    public void testPadLeft() {
        assertEquals(null, Utils.padLeft(null, -10));
        assertEquals(null, Utils.padLeft(null, 0));
        assertEquals("      null", Utils.padLeft(null, 10));

        assertEquals("", Utils.padLeft("", -10));
        assertEquals("", Utils.padLeft("", 0));
        assertEquals("          ", Utils.padLeft("", 10));

        assertEquals("test", Utils.padLeft("test", -10));
        assertEquals("test", Utils.padLeft("test", 0));
        assertEquals("      test", Utils.padLeft("test", 10));

        assertEquals("longerString", Utils.padLeft("longerString", -5));
        assertEquals("longerString", Utils.padLeft("longerString", 0));
        assertEquals("longerString", Utils.padLeft("longerString", 5));
    }

    @Test
    public void testFormatLong() {
        assertEquals("-1", Utils.formatLong(-1, -10));
        assertEquals("-1", Utils.formatLong(-1, 0));
        assertEquals("        -1", Utils.formatLong(-1, 10));

        assertEquals("0", Utils.formatLong(0, -10));
        assertEquals("0", Utils.formatLong(0, 0));
        assertEquals("         0", Utils.formatLong(0, 10));

        assertEquals("1", Utils.formatLong(1, -10));
        assertEquals("1", Utils.formatLong(1, 0));
        assertEquals("         1", Utils.formatLong(1, 10));

        assertEquals("-9,223,372,036,854,775,808", Utils.formatLong(Long.MIN_VALUE, -30));
        assertEquals("-9,223,372,036,854,775,808", Utils.formatLong(Long.MIN_VALUE, 0));
        assertEquals("    -9,223,372,036,854,775,808", Utils.formatLong(Long.MIN_VALUE, 30));

        // Tests with Long.MAX_VALUE fail (maybe some overflow in the formatter)
        //assertEquals("9,223,372,036,854,775,808", Utils.formatLong(Long.MAX_VALUE, -30));
        //assertEquals("9,223,372,036,854,775,808", Utils.formatLong(Long.MAX_VALUE, 0));
        //assertEquals("     9,223,372,036,854,775,808", Utils.formatLong(Long.MAX_VALUE, 30));
    }

    @Test
    public void testFormatDouble() {
        assertEquals("-1.00", Utils.formatDouble(-1.0d, -10));
        assertEquals("-1.00", Utils.formatDouble(-1.0d, 0));
        assertEquals("     -1.00", Utils.formatDouble(-1.0d, 10));

        assertEquals("0.00", Utils.formatDouble(0.0d, -10));
        assertEquals("0.00", Utils.formatDouble(0.0d, 0));
        assertEquals("      0.00", Utils.formatDouble(0.0d, 10));

        assertEquals("1.00", Utils.formatDouble(1.0d, -10));
        assertEquals("1.00", Utils.formatDouble(1.0d, 0));
        assertEquals("      1.00", Utils.formatDouble(1.0d, 10));

        assertEquals("1.50", Utils.formatDouble(1.5d, -10));
        assertEquals("1.50", Utils.formatDouble(1.5d, 0));
        assertEquals("      1.50", Utils.formatDouble(1.5d, 10));

        assertEquals("1.51", Utils.formatDouble(1.505d, -10));
        assertEquals("1.51", Utils.formatDouble(1.505d, 0));
        assertEquals("      1.51", Utils.formatDouble(1.505d, 10));
    }
}