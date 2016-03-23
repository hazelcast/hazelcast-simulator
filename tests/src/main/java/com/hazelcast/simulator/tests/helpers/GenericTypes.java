package com.hazelcast.simulator.tests.helpers;

import com.hazelcast.core.HazelcastInstance;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateIntegerKeys;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArray;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;

public enum GenericTypes {

    INTEGER,
    STRING,
    BYTE;

    public Object[] generateKeys(HazelcastInstance targetInstance, KeyLocality keyLocality, int keyCount, int keySize) {
        switch (this) {
            case INTEGER:
                return generateIntegerKeys(keyCount, keyLocality, targetInstance);
            case STRING:
                return generateStringKeys(keyCount, keySize, keyLocality, targetInstance);
            default:
                throw new UnsupportedOperationException("Unsupported keyType: " + this);
        }
    }

    public Object generateValue(Random random, int valueSize) {
        switch (this) {
            case STRING:
                return generateString(valueSize);
            case BYTE:
                return generateByteArray(random, valueSize);
            default:
                return random.nextInt(valueSize);
        }
    }
}
