package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenericTypesTest {

    private static final int PARTITION_COUNT = 10;

    private static HazelcastInstance hz;

    private Random random = new Random();

    @BeforeClass
    public static void beforeClass() {
        Config config = new Config();
        config.setProperty("hazelcast.partition.count", "" + PARTITION_COUNT);

        hz = Hazelcast.newHazelcastInstance(config);
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testGenerateKeys_withInteger() {
        Object[] keys = GenericTypes.INTEGER.generateKeys(hz, KeyLocality.SHARED, 23, -1);

        assertEquals(23, keys.length);
        for (Object key : keys) {
            assertTrue(key instanceof Integer);
            assertTrue((Integer) key >= 0);
        }
    }

    @Test
    public void testGenerateKeys_withString() {
        Object[] keys = GenericTypes.STRING.generateKeys(hz, KeyLocality.SHARED, 3, 20);

        assertEquals(3, keys.length);
        for (Object key : keys) {
            assertTrue(key instanceof String);
            assertEquals(20, ((String) key).length());
        }
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGenerateKeys_withByte() {
        GenericTypes.BYTE.generateKeys(hz, KeyLocality.SHARED, 3, 20);
    }

    @Test
    public void testGenerateValue_withInteger() {
        Object value = GenericTypes.INTEGER.generateValue(random, 10);

        assertTrue(value instanceof Integer);
        assertTrue((Integer) value < 10);
    }

    @Test
    public void testGenerateValue_withString() {
        Object value = GenericTypes.STRING.generateValue(random, 42);

        assertTrue(value instanceof String);
        assertEquals(42, ((String) value).length());
    }

    @Test
    public void testGenerateValue_withByte() {
        Object value = GenericTypes.BYTE.generateValue(random, 23);

        assertTrue(value instanceof byte[]);
        assertEquals(23, ((byte[]) value).length);
    }
}
