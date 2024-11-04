/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.ucd.map;

import com.hazelcast.collection.IList;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.ucd.UCDTest;

import static org.junit.Assert.assertEquals;

public class TestUserCodeWithIMapPredicate extends UCDTest {

    // properties
    public int keyCount = 10000;
    public KeyLocality keyLocality = KeyLocality.SHARED;

    private IMap<Integer, Integer> map;
    private IList<Long> resultsPerWorker;

    @Override
    @Setup
    public void setUp() throws ReflectiveOperationException {
        super.setUp();
        map = targetInstance.getMap(name);
        resultsPerWorker = targetInstance.getList(name + ":ResultMap");
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, i);
        }
    }

    @TimeStep
    public void timeStep() throws ReflectiveOperationException {
        int returnSize =
                map.values((Predicate<Integer, Integer>) udf.getDeclaredConstructor()
                        .newInstance()).size();
        resultsPerWorker.add((long) returnSize);
    }

    @Verify
    public void verify() {
        int failures = 0;
        for (Long result : resultsPerWorker) {
            if (result % 5000 != 0) {
                failures++;
            }
        }
        assertEquals(0, failures);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        resultsPerWorker.destroy();
    }
}
