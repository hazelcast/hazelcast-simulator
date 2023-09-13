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
package com.hazelcast.simulator.tests.map.prunability;

import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Collection;

public abstract class PredicateCompoundKeyBenchmarkBase extends HazelcastTest {
    // properties
    // the number of map entries
    public int entryCount = 500_000;

    private IMap<PartitionAwareKeyPojo, String> map;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<PartitionAwareKeyPojo, String> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            String value = "" + i;
            PartitionAwareKeyPojo key = new PartitionAwareKeyPojo(i, value, i);
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep
    public void timeStep() throws Exception {
        final int i = prepareKey();
        final PartitionAwareKeyPojo _key = new PartitionAwareKeyPojo(i, "" + i, i);
        Collection<String> values = map.values(
                Predicates.partitionPredicate(_key.getPartitionKey(), Predicates.and(
                        Predicates.equal("__key.a", i), Predicates.equal("__key.x", i))));
        if (values.size() != 1) {
            throw new Exception("wrong entry count");
        }
    }

    abstract protected int prepareKey();

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}

