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
package com.hazelcast.simulator.tests.map;

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.Employee;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;

import java.util.Random;

public class SQLTest extends HazelcastTest {

    // properties
    public int itemCount = 1_000_000;
    public String query;
    // The $MAP will automatically be replaced by the configured map name
    public String where = "__key = ?";

    private IMap<Integer, Object> map;
    private String select;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        select = "SELECT id, name, age FROM " + name + " WHERE " + where;
    }

    @Prepare(global = true)
    public void prepare() {
            targetInstance.getSql().execute("CREATE MAPPING \"" + name + "\" EXTERNAL NAME \"" + name + "\" TYPE IMap\n" +
                    "OPTIONS (\n" +
                    "  'keyFormat' = 'java',\n" +
                    "  'keyJavaClass' = 'java.lang.Integer',\n" +
                    "  'valueFormat' = 'java',\n" +
                    "  'valueJavaClass' = 'com.hazelcast.simulator.tests.map.domain.Employee'\n" +
                    ")");
        Streamer<Integer, Object> streamer = StreamerFactory.getInstance(map);
        Random random = new Random();
        for (int i = 0; i < itemCount; i++) {
            streamer.pushEntry(i, new Employee(i, "" + random.nextLong(), random.nextInt() / 100));
        }
        streamer.await();
    }

    @TimeStep(prob = 1)
    public Employee select(ThreadState state) {
        SqlResult result = null;
        try {
            result = targetInstance.getSql().execute(select, state.randomKey());
            SqlRow row = result.iterator().next();
            int id = (Integer) row.getObject(0);
            String name = (String) row.getObject(1);
            int age = (Integer) row.getObject(2);
            return new Employee(id, name, age);
        } finally {
            if (result != null) {
                result.close();
            }
        }
    }

    @TimeStep(prob = 0)
    public Object get(ThreadState state) {
        return map.get(state.randomKey());
    }

    public class ThreadState extends BaseThreadState {
        private int randomKey() {
            return random.nextInt(itemCount);
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
