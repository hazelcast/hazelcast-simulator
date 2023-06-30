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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
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

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static java.util.Arrays.asList;


public class ScanByPrunedCompositePartialKeyBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    private IMap<KeyPojo, String> map;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
        final List<PartitioningAttributeConfig> attributeConfigs = asList(
                new PartitioningAttributeConfig("a"),
                new PartitioningAttributeConfig("b")
        );
        final MapConfig mapConfig = new MapConfig(name).setPartitioningAttributeConfigs(attributeConfigs);
        targetInstance.getConfig().addMapConfig(mapConfig);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<KeyPojo, String> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            Integer iKey = i;
            Long lKey = (long) i;
            String v = String.format("%010d", iKey);
            KeyPojo keyPojo = new KeyPojo(iKey, v, lKey);
            streamer.pushEntry(keyPojo, v);
        }
        streamer.await();

        SqlService sqlService = targetInstance.getSql();
        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name + " "
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'com.hazelcast.simulator.tests.map.prunability.KeyPojo',\n"
                + "                'valueFormat' = 'java',\n"
                + "                'valueJavaClass' = 'java.lang.String'\n"
                + "        )";

        sqlService.execute(query);
    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        String query = "SELECT __key, this FROM " + name + " WHERE \"__key.a\"= ? AND \"__key.b\"= ?";
        String keyMatch = String.format("%010d", new Random().nextInt(entryCount));
        int actual = 0;
        try (SqlResult result = sqlService.execute(query, keyMatch)) {
            for (SqlRow row : result) {
                Object key = row.getObject(0);
                if (!(key instanceof KeyPojo)) {
                    throw new IllegalStateException("Returned object is not "
                            + IdentifiedDataSerializablePojo.class.getSimpleName() + ": " + key);
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

