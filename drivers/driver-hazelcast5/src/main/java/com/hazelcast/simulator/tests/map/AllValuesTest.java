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

import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.predicates.TruePredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Collection;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiString;
import static org.junit.Assert.assertEquals;

/**
 * A test that verifies the IMap.values() or IMap.values(true-predicate) behavior.
 */
public class AllValuesTest extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 1000000;
    // the size of the key (in chars, since key is string)
    public int keyLength = 10;
    // the size of the value (in chars, since value is a string)
    public int valueLength = 1000;
    // a switch between using IMap.keySet() or IMap.keySet(true-predicate)
    public boolean usePredicate = false;

    private IMap<String, String> map;

    @Setup
    public void setup() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<String, String> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < entryCount; i++) {
            String key = generateAsciiString(keyLength);
            String value = generateAsciiString(valueLength);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep
    public void timeStep() throws Exception {
        // todo: it would be better to have 2 timestep methods so that optimized code can be generated.
        Collection<String> result;
        if (usePredicate) {
            result = map.values(TruePredicate.INSTANCE);
        } else {
            result = map.values();
        }

        assertEquals(entryCount, result.size());
    }

    @Teardown
    public void teardown() {
        map.destroy();
    }
}
