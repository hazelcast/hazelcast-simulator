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
}