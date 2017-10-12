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
