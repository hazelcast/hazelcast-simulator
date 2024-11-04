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

import java.text.MessageFormat;
import java.util.List;

public class Client1Query3Benchmark extends AbstractClient1Benchmark {
    private final String QUERY =
            "select\n" +
                    "      t.string2 as name,\n" +
                    "       SUM(long2) as s_long2,\n" +
                    "       SUM(long1) as s_long1v,\n" +
                    "       SUM(int2) as s_int2\n" +
                    " FROM " + name + " t\n" +
                    " WHERE monthStr = ?\n" +
                    " AND jobId = ? \n" +
                    " AND region in {0} \n" +
                    " GROUP BY t.string2 ORDER BY s_long2 DESC;\n";

        private final ThreadLocal<MessageFormat> mf = ThreadLocal.withInitial(() -> new MessageFormat(QUERY));

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
            List<Integer> randomRegionList = getRandomRegionList();
            boolean first = true;
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            for (Integer integer : randomRegionList) {
                if (!first) {
                    builder.append(",");
                } else {
                    first = false;
                }
                builder.append("?");
            }
            builder.append(")");
            String query = mf.get().format(new Object[]{builder.toString()});
            System.out.println(query);
            Object[] args = new Object[2 + randomRegionList.size()];
            int i = 0;
            args[i++] = getRandomMonth();
            args[i++] = JOB_ID;
            for (Integer integer : randomRegionList) {
                args[i++] = integer;
            }
            try (SqlResult result = sqlService.execute(query, args)) {
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

