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
package com.hazelcast.simulator.tests.map.sql.realprod.client1;

import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

public class Client1Query1Benchmark extends AbstractClient1Benchmark {
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
        super.setUp();
    }

    @Prepare(global = true)
    public void prepare() {
        super.prepare();
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


    @Teardown
    public void tearDown() {
        super.tearDown();
    }
}

