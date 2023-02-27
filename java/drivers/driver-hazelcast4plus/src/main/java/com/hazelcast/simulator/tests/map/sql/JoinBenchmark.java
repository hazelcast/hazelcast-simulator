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
package com.hazelcast.simulator.tests.map.sql;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.IdentifiedDataWithLongSerializablePojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Arrays;
import java.util.Random;


public class JoinBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int leftEntryCount = 1000;
    public int rightEntryCount = 10_000_000;
    public int arraySize = 20;

    public boolean leftKey = true;
    public boolean rightKey = true;
    public boolean useIndices = true;

    //16 byte + N*(20*N
    private IMap<Integer, IdentifiedDataWithLongSerializablePojo> map;
    private IMap<Integer, IdentifiedDataWithLongSerializablePojo> map2;

    private String name2 = name + "2";

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
        this.map2 = targetInstance.getMap(name2);
        if (useIndices) {
            map.addIndex(IndexType.HASH, "value");
            map2.addIndex(IndexType.HASH, "value");
        }
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, IdentifiedDataWithLongSerializablePojo> streamer = StreamerFactory.getInstance(map);
        Streamer<Integer, IdentifiedDataWithLongSerializablePojo> streamer2 = StreamerFactory.getInstance(map2);
        Integer[] sampleArray = new Integer[arraySize];
        for (int i = 0; i < arraySize; i++) {
            sampleArray[i] = i;
        }

        (new Random()).ints(0, rightEntryCount)
                .distinct()
                .limit(leftEntryCount)
                .forEach(i -> {
                    Integer key = i;
                    IdentifiedDataWithLongSerializablePojo value =
                            new IdentifiedDataWithLongSerializablePojo(sampleArray, key.longValue());
                    streamer.pushEntry(key, value);
                });
        for (int i = 0; i < rightEntryCount; i++) {
            Integer key = i;
            IdentifiedDataWithLongSerializablePojo value =
                    new IdentifiedDataWithLongSerializablePojo(sampleArray, key.longValue());
            streamer2.pushEntry(key, value);
        }
        streamer.await();
        streamer2.await();
        SqlService sqlService = targetInstance.getSql();

        Arrays.asList(new String[]{name, name2}).forEach(aName -> {
            String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + aName + " "
                    + "EXTERNAL NAME " + aName + " "
                    + "        TYPE IMap\n"
                    + "        OPTIONS (\n"
                    + "                'keyFormat' = 'java',\n"
                    + "                'keyJavaClass' = 'java.lang.Integer',\n"
                    + "                'valueFormat' = 'java',\n"
                    + "                'valueJavaClass' = 'com.hazelcast.simulator.hz.IdentifiedDataWithLongSerializablePojo'\n"
                    + "        )";

            sqlService.execute(query);
        });
    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        String query = "SELECT * FROM " + name + " AS T1 JOIN " + name2 + " AS T2 ON " +
                (leftKey ? "T1.__key" : "T1.\"value\"") + " = " + (rightKey ? "T2.__key" : "T2.\"value\"");

        try (SqlResult result = sqlService.execute(query)) {
            int rowCount = 0;
            for (SqlRow row : result) {
                rowCount++;
            }
            if (rowCount != leftEntryCount) {
                throw new IllegalArgumentException("Invalid row count [expected=1 , actual=" + rowCount + "]");
            }
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}

