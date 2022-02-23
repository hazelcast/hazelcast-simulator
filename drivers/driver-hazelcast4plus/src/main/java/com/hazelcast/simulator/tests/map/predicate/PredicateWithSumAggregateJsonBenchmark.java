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
package com.hazelcast.simulator.tests.map.predicate;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;


public class PredicateWithSumAggregateJsonBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    private long sum = 0;

    //16 byte + N*(20*N
    private IMap<Integer, HazelcastJsonValue> map;

    @Setup
    public void setup() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, HazelcastJsonValue> streamer = StreamerFactory.getInstance(map);

        StringBuilder suffix = new StringBuilder();
        for (int j = 0; j < 31; j++) {
            suffix.append(", \"value").append(j).append("\":\"").append(j).append("\"");
        }
        suffix.append("}");

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            StringBuilder builder = new StringBuilder();
            builder.append("{").append("\"id\":").append(i).append(",\"value\":").append(i).append(suffix);
            HazelcastJsonValue jsonValue = new HazelcastJsonValue(builder.toString());
            sum += i;
            streamer.pushEntry(key, jsonValue);
        }
        streamer.await();
    }

    @TimeStep
    public void timeStep() throws Exception {
        Long sum = map.aggregate(Aggregators.longSum("value"));

        if (sum != this.sum) {
            throw new IllegalArgumentException("Invalid sum [expected=" + this.sum + ", actual=" + sum + "]");
        }
    }

    @Teardown
    public void teardown() {
        map.destroy();
    }
}

