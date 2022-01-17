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
package com.hazelcast.simulator.benchmarks.sql;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;


public class ScanWithAggregateBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    //16 byte + N*(20*N
    private IMap<Integer, IdentifiedDataSerializablePojo> map;

    @Setup
    public void setup() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, IdentifiedDataSerializablePojo> streamer = StreamerFactory.getInstance(map);
        Integer[] sampleArray = new Integer[20];
        for (int i = 0; i < 20; i++) {
            sampleArray[i] = i;
        }

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            IdentifiedDataSerializablePojo value = new IdentifiedDataSerializablePojo(sampleArray, String.format("%010d", key));
            streamer.pushEntry(key, value);
        }
        streamer.await();

        SqlService sqlService = targetInstance.getSql();
        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " " +
                "EXTERNAL NAME " + name + " " +
                "        TYPE IMap\n" +
                "        OPTIONS (\n" +
                "                'keyFormat' = 'java',\n" +
                "                'keyJavaClass' = 'java.lang.Integer',\n" +
                "                'valueFormat' = 'java',\n" +
                "                'valueJavaClass' = 'com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo'\n" +
                "        )";

        sqlService.execute(query);

    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        String query = "SELECT COUNT(*) FROM " + name ;
        try (SqlResult result = sqlService.execute(query)) {
            int rowCount = 0;
            for (SqlRow row : result) {
                long count = row.getObject(0);
                if (count != entryCount) {
                    throw new IllegalArgumentException("Invalid count [expected=" + entryCount + ", actual=" + count + "]");
                }
                rowCount++;
            }
            if (rowCount != 1) {
                throw new IllegalArgumentException("Invalid row count [expected=1 , actual=" + rowCount + "]");
            }
        }
    }

    @Teardown
    public void teardown() {
        map.destroy();
    }
}

