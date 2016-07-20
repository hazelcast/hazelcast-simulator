/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.IMap;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.SortedMap;
import java.util.TreeMap;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;

/**
 * Test for {@link IMap#putAll(java.util.Map)} which creates the input values on the fly during the RUN phase.
 * <p>
 * You can configure the {@link #batchSize} to determine the number of inserted values per operation.
 * You can configure the {@link #keyRange} to determine the key range for inserted values.
 */
public class MapPutAllOnTheFlyTest extends AbstractTest {

    // properties
    public int batchSize = 10;
    public int keyRange = 1000000;

    private IMap<Integer, Integer> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @TimeStep
    protected void timeStep(BaseThreadState state) throws Exception {
        SortedMap<Integer, Integer> values = new TreeMap<Integer, Integer>();
        for (int i = 0; i < batchSize; i++) {
            int key = state.randomInt(keyRange);

            values.put(key, key);
        }

        map.putAll(values);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }
}
