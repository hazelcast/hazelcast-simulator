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

import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Date;
import java.util.Random;


public class QueryPlanBenchmark extends HazelcastTest {
    // properties
    // the number of mapping entries
    public int entryCount = 1000;

    @Setup
    public void setUp() {
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < entryCount; i++) {
            if (i % 100 == 0) {
                System.out.println((new Date()) + " - Creating mapping no: " + i);
            }
            String name = "map" + i;
            targetInstance.getMap(name);
            SqlService sqlService = targetInstance.getSql();
            String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                    + "EXTERNAL NAME " + name + " "
                    + "        TYPE IMap\n"
                    + "        OPTIONS (\n"
                    + "                'keyFormat' = 'java',\n"
                    + "                'keyJavaClass' = 'java.lang.Integer',\n"
                    + "                'valueFormat' = 'java',\n"
                    + "                'valueJavaClass' = 'com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo'\n"
                    + "        )";

            sqlService.execute(query);
        }
    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();

        int key = new Random().nextInt(entryCount);
        String query = "SELECT __key, this FROM map0 WHERE CAST(\"valueField\" AS TINYINT) = " + key;
        int actual = 0;
        try (SqlResult result = sqlService.execute(query)) {
            for (SqlRow row : result) {
                Object value = row.getObject(1);
                if (!(value instanceof IdentifiedDataSerializablePojo)) {
                    throw new IllegalStateException("Returned object is not "
                            + IdentifiedDataSerializablePojo.class.getSimpleName() + ": " + value);
                }
                actual++;
            }
        }

        if (actual != 0) {
            throw new IllegalArgumentException("Invalid count [expected=" + 0 + ", actual=" + actual + "]");
        }
    }

    @Teardown
    public void tearDown() {
        for (int i = 0; i < entryCount; i++) {
            String name = "map" + i;
            targetInstance.getMap(name).destroy();
        }
    }
}

