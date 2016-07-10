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

import com.hazelcast.core.IMap;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.tests.map.helpers.DataSerializableEmployee;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.util.Random;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateString;
import static com.hazelcast.simulator.worker.metronome.MetronomeFactory.withFixedIntervalMs;

public class SqlPredicateTest extends AbstractTest {

    private static final String[] NAMES = {"aaa", "bbb", "ccc", "ddd", "eee", "fff", "ggg"};

    // properties
    public int keyCount = 10000;
    public int keyLength = 10;
    public String sql = "age = 30 AND active = true";
    public MetronomeType metronomeType = MetronomeType.SLEEPING;
    public int intervalMs = 0;
    public int maxAge = 75;
    public double maxSalary = 1000.0;

    private IMap<String, DataSerializableEmployee> map;

    @Setup
    public void setup() {
        this.map = targetInstance.getMap(name);
    }

    @Warmup(global = true)
    public void warmup() {
        Random random = new Random();
        Streamer<String, DataSerializableEmployee> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            String key = generateString(keyLength);
            DataSerializableEmployee value = generateRandomEmployee(random);
            streamer.pushEntry(key, value);
        }
        streamer.await();
        logger.info("Map size is: " + map.size());
        logger.info("Map localKeySet size is: " + map.localKeySet().size());
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        private SqlPredicate sqlPredicate = new SqlPredicate(sql);
        private Metronome metronome = withFixedIntervalMs(intervalMs, metronomeType);

        @Override
        protected void timeStep() throws Exception {
            metronome.waitForNext();
            map.values(sqlPredicate);
        }
    }

    private DataSerializableEmployee generateRandomEmployee(Random random) {
        int id = random.nextInt();
        String name = NAMES[random.nextInt(NAMES.length)];
        int age = random.nextInt(maxAge);
        boolean active = random.nextBoolean();
        double salary = random.nextDouble() * maxSalary;
        return new DataSerializableEmployee(id, name, age, active, salary);
    }

    @Teardown
    public void teardown() {
        map.destroy();
        logger.info(getOperationCountInformation(targetInstance));
    }

}
