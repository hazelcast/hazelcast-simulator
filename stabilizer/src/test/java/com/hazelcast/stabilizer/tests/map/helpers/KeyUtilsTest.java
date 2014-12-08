package com.hazelcast.stabilizer.tests.map.helpers;

import com.hazelcast.stabilizer.tests.utils.KeyLocality;
import org.junit.Test;

import static com.hazelcast.stabilizer.tests.map.helpers.KeyUtils.generateInt;
import static com.hazelcast.stabilizer.tests.map.helpers.KeyUtils.generateStringKey;
import static org.junit.Assert.assertEquals;

public class KeyUtilsTest {

    @Test
    public void generateInt_singlePartition() {
        int key1 = generateInt(100, KeyLocality.SinglePartition, null);
        int key2 = generateInt(100, KeyLocality.SinglePartition, null);

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
