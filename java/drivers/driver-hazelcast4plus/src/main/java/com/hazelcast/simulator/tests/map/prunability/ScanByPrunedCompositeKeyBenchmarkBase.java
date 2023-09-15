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
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

public abstract class ScanByPrunedCompositeKeyBenchmarkBase extends HazelcastTest {
    // properties
    // the number of map entries
    public int entryCount = 500_000;

    private IMap<KeyPojo, String> map;
    private SqlService sqlService;
    private String query;

    @Setup
    public void setUp() {
        this.name = prepareName();
        this.map = targetInstance.getMap(name);
        this.query = "SELECT this FROM " + name + " WHERE a = ? AND x = ?";
        this.sqlService = targetInstance.getSql();
    }

    @Prepare(global = true)
    public void prepare() {
        SqlService sqlService = targetInstance.getSql();
        Streamer<KeyPojo, String> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            String v = "" + i;
            KeyPojo keyPojo = new KeyPojo(i, v, i);
            streamer.pushEntry(keyPojo, v);
        }
        streamer.await();

        String createMappingQuery = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name + " "
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'com.hazelcast.simulator.tests.map.prunability.KeyPojo',\n"
                + "                'valueFormat' = 'java',\n"
                + "                'valueJavaClass' = 'java.lang.String'\n"
                + "        )";

        sqlService.execute(createMappingQuery);

        int size = map.size();
        if (size != entryCount) {
            throw new IllegalArgumentException("Invalid map size : " + size + ", when expecting " + entryCount);
        }
    }

    @TimeStep
    public void timeStep() throws Exception {
        final int i = prepareKey();
        try (SqlResult result = sqlService.execute(query, i, i)) {
            int rowCount = 0;
            for (SqlRow ignored : result) {
                rowCount++;
            }
            if (rowCount != 1) {
                throw new IllegalArgumentException("Invalid count [expected=" + 1 + ", actual=" + rowCount + "]");
            }
        }
    }

    /**
     * @return map name to scan
     */
    protected abstract String prepareName();

    protected abstract int prepareKey();

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
