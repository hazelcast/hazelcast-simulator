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
package com.hazelcast.simulator.tests.map.sql.realprod;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.text.DateFormatSymbols;

public class Client1Query1Benchmark extends HazelcastTest {
    private static final long JOB_ID = 2885;

    // properties
    // the number of map entries
    public int entryCount = 1000;

    private IMap<String, Client1ModelClass2> map;

    private final String QUERY = "" +
            "select distinct monthStr from \"" + name + "\" where jobId = ? " +
            "order by\n" +
            "         case substring(monthStr,1,3)\n" +
            "           when 'Jan' then 1\n" +
            "           when 'Feb' then 2\n" +
            "           when 'Mar' then 3\n" +
            "           when 'Apr' then 4\n" +
            "           when 'May' then 5\n" +
            "           when 'Jun' then 6\n" +
            "           when 'Jul' then 7\n" +
            "           when 'Aug' then 8\n" +
            "           when 'Sep' then 9\n" +
            "           when 'Oct' then 10\n" +
            "           when 'Nov' then 11\n" +
            "           when 'Dec' then 12\n" +
            "             end";

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
        map.addIndex(IndexType.HASH, "jobId");
        map.addIndex(IndexType.HASH, "jobId", "monthStr");
        map.addIndex(IndexType.HASH, "jobId", "monthStr", "region");
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<String, Client1ModelClass2> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            Client1ModelClass2 class2 = generateRandomTldAcquirer(i);
            map.put(class2.getKey(), class2);
            streamer.pushEntry(class2.getKey(), class2);
        }
        streamer.await();
        SqlService sqlService = targetInstance.getSql();

        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name + " "
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'java.lang.String',\n"
                + "                'valueFormat' = 'java',\n"
                + "                'valueJavaClass' = 'com.hazelcast.simulator.tests.map.sql.realprod.Client1ModelClass2'\n"
                + "        )";

        sqlService.execute(query);
    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        try (SqlResult result = sqlService.execute(QUERY, JOB_ID)) {
            int rowCount = 0;
            for (SqlRow row : result) {
                rowCount++;
            }
        }
    }

    private static Client1ModelClass2 generateRandomTldAcquirer(int counter) {

        Client1ModelClass2 t = new Client1ModelClass2();

        int monthInt = RandomUtils.nextInt(0,  12);
        String month = getShortMonth(monthInt);
        int year = RandomUtils.nextInt(0, 22) + 2000;

        t.setMonthStr(month + " " + year);
        t.setJobId(JOB_ID);
        t.setRegion(RandomUtils.nextInt(0, 12));
        t.setLong3(RandomUtils.nextLong(0, Long.MAX_VALUE));
        t.setLong4(RandomUtils.nextLong(0, Long.MAX_VALUE));
        t.setString2(RandomStringUtils.random(3, "abc"));
        t.setInt2(RandomUtils.nextInt(0, 1_000_000));

        t.setString1(RandomStringUtils.random(3, "def"));
        t.setInt1(RandomUtils.nextInt(0, 1000));

        return t;
    }

    private static String getRandomMonth() {
        String month = getShortMonth(RandomUtils.nextInt(0, 12));
        int year = RandomUtils.nextInt(0, 22) + 2000;
        return month + " " + year;
    }

    private static String getShortMonth(int monthIndex) {
        return new DateFormatSymbols().getShortMonths()[monthIndex];
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}

