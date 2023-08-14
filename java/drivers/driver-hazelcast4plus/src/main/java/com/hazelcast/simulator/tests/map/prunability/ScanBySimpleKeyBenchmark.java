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

import java.util.Random;

public class ScanBySimpleKeyBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    private IMap<Integer, String> map;
    private SqlService sqlService;
    private String query;

    @Setup
    public void setUp() {
        this.sqlService = targetInstance.getSql();
        this.map = targetInstance.getMap(name);
        // enables partition pruning with FullScan rel in plan.
        this.query = "SELECT this FROM " + name + " WHERE __key = ? AND this IS NOT NULL";
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, String> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            String v = String.format("%010d", i);
            streamer.pushEntry(i, v);
        }
        streamer.await();

        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name + " "
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'java.lang.Integer',\n"
                + "                'valueFormat' = 'java',\n"
                + "                'valueJavaClass' = 'java.lang.String'\n"
                + "        )";

        sqlService.execute(query);
    }

    @TimeStep
    public void timeStep() throws Exception {
        int actual = 0;
        int key = new Random().nextInt(entryCount);
        try (SqlResult result = sqlService.execute(query, key)) {
            for (SqlRow row : result) {
                Object value = row.getObject(0);
                if (!(value instanceof String)) {
                    throw new IllegalStateException("Returned object is not "
                            + String.class.getSimpleName() + ": " + value);
                }
                actual++;
            }
        }

        if (actual != 1) {
            throw new IllegalArgumentException("Invalid count [expected=" + 1 + ", actual=" + actual + "]");
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}

