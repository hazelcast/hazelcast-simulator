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
import com.hazelcast.simulator.hz.LongCompactPojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.math.BigDecimal;

public class ScanWithSumAggregateCompactBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    private long sum;

    //16 byte + N*(20*N
    private IMap<Integer, LongCompactPojo> map;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, LongCompactPojo> streamer = StreamerFactory.getInstance(map);
        Integer[] sampleArray = new Integer[20];
        for (int i = 0; i < 20; i++) {
            sampleArray[i] = i;
        }

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            LongCompactPojo value = new LongCompactPojo(sampleArray, key.longValue());
            sum += i;
            streamer.pushEntry(key, value);
        }
        streamer.await();

        SqlService sqlService = targetInstance.getSql();
        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name
                + " (  \"numbers-0\" INTEGER EXTERNAL NAME \"this.numbers-0\",\n" +
                "  \"numbers-1\" INTEGER EXTERNAL NAME \"this.numbers-1\",\n" +
                "  \"numbers-10\" INTEGER EXTERNAL NAME \"this.numbers-10\",\n" +
                "  \"numbers-11\" INTEGER EXTERNAL NAME \"this.numbers-11\",\n" +
                "  \"numbers-12\" INTEGER EXTERNAL NAME \"this.numbers-12\",\n" +
                "  \"numbers-13\" INTEGER EXTERNAL NAME \"this.numbers-13\",\n" +
                "  \"numbers-14\" INTEGER EXTERNAL NAME \"this.numbers-14\",\n" +
                "  \"numbers-15\" INTEGER EXTERNAL NAME \"this.numbers-15\",\n" +
                "  \"numbers-16\" INTEGER EXTERNAL NAME \"this.numbers-16\",\n" +
                "  \"numbers-17\" INTEGER EXTERNAL NAME \"this.numbers-17\",\n" +
                "  \"numbers-18\" INTEGER EXTERNAL NAME \"this.numbers-18\",\n" +
                "  \"numbers-19\" INTEGER EXTERNAL NAME \"this.numbers-19\",\n" +
                "  \"numbers-2\" INTEGER EXTERNAL NAME \"this.numbers-2\",\n" +
                "  \"numbers-3\" INTEGER EXTERNAL NAME \"this.numbers-3\",\n" +
                "  \"numbers-4\" INTEGER EXTERNAL NAME \"this.numbers-4\",\n" +
                "  \"numbers-5\" INTEGER EXTERNAL NAME \"this.numbers-5\",\n" +
                "  \"numbers-6\" INTEGER EXTERNAL NAME \"this.numbers-6\",\n" +
                "  \"numbers-7\" INTEGER EXTERNAL NAME \"this.numbers-7\",\n" +
                "  \"numbers-8\" INTEGER EXTERNAL NAME \"this.numbers-8\",\n" +
                "  \"numbers-9\" INTEGER EXTERNAL NAME \"this.numbers-9\",\n" +
                "  \"numbers-present-0\" BOOLEAN EXTERNAL NAME \"this.numbers-present-0\",\n" +
                "  \"numbers-present-1\" BOOLEAN EXTERNAL NAME \"this.numbers-present-1\",\n" +
                "  \"numbers-present-10\" BOOLEAN EXTERNAL NAME \"this.numbers-present-10\",\n" +
                "  \"numbers-present-11\" BOOLEAN EXTERNAL NAME \"this.numbers-present-11\",\n" +
                "  \"numbers-present-12\" BOOLEAN EXTERNAL NAME \"this.numbers-present-12\",\n" +
                "  \"numbers-present-13\" BOOLEAN EXTERNAL NAME \"this.numbers-present-13\",\n" +
                "  \"numbers-present-14\" BOOLEAN EXTERNAL NAME \"this.numbers-present-14\",\n" +
                "  \"numbers-present-15\" BOOLEAN EXTERNAL NAME \"this.numbers-present-15\",\n" +
                "  \"numbers-present-16\" BOOLEAN EXTERNAL NAME \"this.numbers-present-16\",\n" +
                "  \"numbers-present-17\" BOOLEAN EXTERNAL NAME \"this.numbers-present-17\",\n" +
                "  \"numbers-present-18\" BOOLEAN EXTERNAL NAME \"this.numbers-present-18\",\n" +
                "  \"numbers-present-19\" BOOLEAN EXTERNAL NAME \"this.numbers-present-19\",\n" +
                "  \"numbers-present-2\" BOOLEAN EXTERNAL NAME \"this.numbers-present-2\",\n" +
                "  \"numbers-present-3\" BOOLEAN EXTERNAL NAME \"this.numbers-present-3\",\n" +
                "  \"numbers-present-4\" BOOLEAN EXTERNAL NAME \"this.numbers-present-4\",\n" +
                "  \"numbers-present-5\" BOOLEAN EXTERNAL NAME \"this.numbers-present-5\",\n" +
                "  \"numbers-present-6\" BOOLEAN EXTERNAL NAME \"this.numbers-present-6\",\n" +
                "  \"numbers-present-7\" BOOLEAN EXTERNAL NAME \"this.numbers-present-7\",\n" +
                "  \"numbers-present-8\" BOOLEAN EXTERNAL NAME \"this.numbers-present-8\",\n" +
                "  \"numbers-present-9\" BOOLEAN EXTERNAL NAME \"this.numbers-present-9\",\n" +
                "  \"numbers-size\" INTEGER EXTERNAL NAME \"this.numbers-size\",\n" +
                "  \"value\" BIGINT EXTERNAL NAME \"this.value\"\n"
                + ")"
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
        String query = "SELECT sum(\"value\") FROM " + name;
        try (SqlResult result = sqlService.execute(query)) {
            int rowCount = 0;
            for (SqlRow row : result) {
                long sum = ((BigDecimal) row.getObject(0)).longValue();

                // That can fail with more than one client, feel free to remove it if needed.
                if (sum != this.sum) {
                    throw new IllegalArgumentException("Invalid sum [expected=" + this.sum + ", actual=" + sum + "]");
                }
                rowCount++;
            }
            if (rowCount != 1) {
                throw new IllegalArgumentException("Invalid row count [expected=1 , actual=" + rowCount + "]");
            }
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}

