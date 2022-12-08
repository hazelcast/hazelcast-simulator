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

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.MultiFieldCompactPojo;
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


public class ScanByKey1CompactEntryBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    //16 byte + N*(20*N
    private IMap<Integer, MultiFieldCompactPojo> map;
    private volatile Object blackhole;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, MultiFieldCompactPojo> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            MultiFieldCompactPojo pojo = new MultiFieldCompactPojo();
            pojo.str1 = i + "-" + 1;
            pojo.str2 = i + "-" + 2;
            pojo.str3 = i + "-" + 3;
            pojo.str4 = i + "-" + 4;
            pojo.str5 = i + "-" + 5;

            pojo.int1 = i + 1;
            pojo.int2 = i + 2;
            pojo.int3 = i + 3;
            pojo.int4 = i + 4;
            pojo.int5 = i + 5;

            pojo.long1 = i  + 1L;
            pojo.long2 = i  + 2L;
            pojo.long3 = i  + 3L;
            pojo.long4 = i  + 4L;
            pojo.long5 = i  + 5L;

            pojo.bool1 = false;
            pojo.bool2 = true;
            pojo.bool3 = false;
            pojo.bool4 = true;
            pojo.bool5 = false;

            streamer.pushEntry(key, pojo);
        }
        streamer.await();

        SqlService sqlService = targetInstance.getSql();
        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name + " "
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'java.lang.Integer',\n"
                + "                'valueFormat' = 'compact',\n"
                + "                'valueCompactTypeName' = 'longCompactPojo'\n"
                + "        )";

        sqlService.execute(query);
    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();

        String query = "SELECT " +
                "str1, str2, str3, str4, str5, " +
                "long1, long2, long3, long4, long5, " +
                "int1, int2, int3, int4, int5, " +
                "bool1, bool2, bool3, bool4, bool5 " +
                " FROM " + name + " WHERE __key = ?";
        int key = new Random().nextInt(entryCount);
        int actual = 0;
       
        try (SqlResult result = sqlService.execute(query, key)) {
            for (SqlRow row : result) {
                blackhole = row.getObject(1);
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

