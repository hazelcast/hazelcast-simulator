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
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.map.domain.DomainObject;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.log4j.Level;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class SerializationStrategyTest extends HazelcastTest {

    public enum Strategy {
        PORTABLE,
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE
    }

    // properties
    public Strategy strategy = Strategy.PORTABLE;
    public int itemCount = 1000000;
    public int recordsPerUnique = 10000;

    private final ThrottlingLogger throttlingLogger = ThrottlingLogger.newLogger(logger, 5000);
    private IMap<String, DomainObject> map;
    private Set<String> uniqueStrings;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
        uniqueStrings = targetInstance.getSet(name);
    }

    @Prepare(global = true)
    public void prepare() {
        int uniqueStringsCount = itemCount / recordsPerUnique;
        String[] strings = generateUniqueStrings(uniqueStringsCount);

        Streamer<String, DomainObject> streamer = StreamerFactory.getInstance(map);
        DomainObjectFactory objectFactory = DomainObjectFactory.newFactory(strategy);
        for (int i = 0; i < itemCount; i++) {
            String indexedField = strings[RandomUtils.nextInt(0, uniqueStringsCount)];
            DomainObject o = createNewDomainObject(objectFactory, indexedField);
            streamer.pushEntry(o.getKey(), o);
        }
        streamer.await();
    }

    private String[] generateUniqueStrings(int uniqueStringsCount) {
        Set<String> stringsSet = new HashSet<>(uniqueStringsCount);
        do {
            String randomString = RandomStringUtils.randomAlphabetic(30);
            stringsSet.add(randomString);
        } while (stringsSet.size() != uniqueStringsCount);
        uniqueStrings.addAll(stringsSet);
        return stringsSet.toArray(new String[uniqueStringsCount]);
    }

    private DomainObject createNewDomainObject(DomainObjectFactory objectFactory, String indexedField) {
        DomainObject o = objectFactory.newInstance();
        o.setKey(randomAlphanumeric(7));
        o.setStringVal(indexedField);
        o.setIntVal(nextInt(0, Integer.MAX_VALUE));
        o.setLongVal(nextLong(0, Long.MAX_VALUE));
        o.setDoubleVal(nextDouble(0.0, Double.MAX_VALUE));
        return o;
    }

    @BeforeRun
    public void beforeRun(ThreadState state) throws Exception {
        state.localUniqueStrings = uniqueStrings.toArray(new String[uniqueStrings.size()]);
    }

    @TimeStep(prob = -1)
    public void getByKey() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @TimeStep(prob = 0)
    public void getByIntIndex() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @TimeStep(prob = 1)
    public void getByStringIndex(ThreadState state) {
        String string = state.getUniqueString();
        Predicate predicate = Predicates.equal("stringVal", string);
        Set<Map.Entry<String, DomainObject>> entries = map.entrySet(predicate);
        throttlingLogger.log(Level.INFO, "GetByStringIndex: " + entries.size() + " entries");
    }

    public class ThreadState extends BaseThreadState {

        private String[] localUniqueStrings;

        private String getUniqueString() {
            int i = randomInt(localUniqueStrings.length);
            return localUniqueStrings[i];
        }
    }
}
