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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.impl.PredicateBuilderImpl;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.io.IOException;

/**
 * A very basic test that benchmarks different forms of getting data, through a get or query etc
 */
public class MapGetVsQueryTest extends HazelcastTest {

    public int itemCount = 100000;

    private IMap<Integer, Pojo> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void globalPrepare() {
        initMap();
    }

    private void initMap() {
        Streamer<Integer, Pojo> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < itemCount; i++) {
            Pojo pojo = new Pojo(i);
            streamer.pushEntry(pojo.id, pojo);
        }
        streamer.await();
    }

    @TimeStep(prob = 0)
    public void predicateBuilder(ThreadState state) {
        int id = state.randomInt(itemCount);
        PredicateBuilder.EntryObject entryObject = new PredicateBuilderImpl().getEntryObject();
        Predicate predicate = entryObject.get("id").equal(id);
        map.values(predicate);
    }

    @TimeStep(prob = 0)
    public void sqlString(ThreadState state) {
        int id = state.randomInt(itemCount);
        SqlPredicate predicate = new SqlPredicate("id=" + id);
        map.values(predicate);
    }

    @TimeStep(prob = 1.0)
    public void get(ThreadState state) {
        int key = state.randomInt(itemCount);
        map.get(key);
    }

    @TimeStep(prob = 0.0)
    public void update(ThreadState state) {
        int id = state.randomInt(itemCount);
        Pojo pojo = new Pojo(id);
        map.put(id, pojo);
    }

    public class ThreadState extends BaseThreadState {
    }

    public static class Pojo implements DataSerializable {
        private int id;

        public Pojo() {
        }

        public Pojo(int id) {
            this.id = id;
        }

        public int getId() {
            return id;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(id);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readInt();
        }
    }
}
