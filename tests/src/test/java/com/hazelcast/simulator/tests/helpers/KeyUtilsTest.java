package com.hazelcast.simulator.tests.helpers;

import org.junit.Test;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntKey;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKey;
import static org.junit.Assert.assertEquals;

public class KeyUtilsTest {

    @Test
    public void generateInt_singlePartition() {
        int key1 = generateIntKey(100, KeyLocality.SinglePartition, null);
        int key2 = generateIntKey(100, KeyLocality.SinglePartition, null);

        assertEquals(0, key1);
        assertEquals(0, key2);
    }

    @Test
    public void generateString_singlePartition() {
        String key1 = generateStringKey(100, KeyLocality.SinglePartition, null);
        String key2 = generateStringKey(100, KeyLocality.SinglePartition, null);

        assertEquals("", key1);
        assertEquals("", key2);
    }
}
