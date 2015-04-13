package com.hazelcast.simulator.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class KeyValuePairTest {

    private final KeyValuePair<String, Integer> keyValuePair = new KeyValuePair<String, Integer>("test", 5);
    private final KeyValuePair<String, Integer> keyValuePairNullKey = new KeyValuePair<String, Integer>(null, -2);
    private final KeyValuePair<String, Integer> keyValuePairNullValue = new KeyValuePair<String, Integer>("test", null);

    @Test
    public void testGet() throws Exception {
        String key = keyValuePair.getKey();
        int value = keyValuePair.getValue();

        assertEquals("test", key);
        assertEquals(5, value);
    }

    @Test
    public void testGet_nullKey() throws Exception {
        String key = keyValuePairNullKey.getKey();
        int value = keyValuePairNullKey.getValue();

        assertNull(key);
        assertEquals(-2, value);
    }

    @Test
    public void testGet_nullValue() throws Exception {
        String key = keyValuePairNullValue.getKey();
        Integer value = keyValuePairNullValue.getValue();

        assertEquals("test", key);
        assertNull(value);
    }

    @Test
    public void testEquals() throws Exception {
        KeyValuePair<String, Integer> other = new KeyValuePair<String, Integer>("test", 5);

        assertEquals(keyValuePair, other);
    }

    @Test
    public void testEquals_self() throws Exception {
        assertEquals(keyValuePair, keyValuePair);
    }

    @Test
    public void testEquals_null() throws Exception {
        assertNotEquals(keyValuePair, null);
    }

    @Test
    public void testEquals_nullKey() throws Exception {
        assertNotEquals(keyValuePair, keyValuePairNullKey);
        assertNotEquals(keyValuePairNullKey, keyValuePair);
    }

    @Test
    public void testEquals_nullValue() throws Exception {
        assertNotEquals(keyValuePair, keyValuePairNullValue);
        assertNotEquals(keyValuePairNullValue, keyValuePair);
    }

    @Test
    public void testHashCode() throws Exception {
        KeyValuePair<String, Integer> other = new KeyValuePair<String, Integer>(null, -2);

        assertEquals(keyValuePairNullKey.hashCode(), other.hashCode());
    }
}
