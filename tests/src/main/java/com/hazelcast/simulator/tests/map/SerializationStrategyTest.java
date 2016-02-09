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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.tests.map.domain.DomainObject;
import com.hazelcast.simulator.tests.map.domain.DomainObjectFactory;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomUtils.nextDouble;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

public class SerializationStrategyTest {

    private enum Operation {
        GET_BY_KEY,
        GET_BY_INT_INDEX,
        GET_BY_STRING_INDEX
    }

    public enum Strategy {
        PORTABLE,
        SERIALIZABLE,
        DATA_SERIALIZABLE,
        IDENTIFIED_DATA_SERIALIZABLE
    }

    private static final ILogger LOGGER = Logger.getLogger(SerializationStrategyTest.class);
    private static final ThrottlingLogger THROTTLING_LOGGER = ThrottlingLogger.newLogger(LOGGER, 5000);

    // properties
    public String basename = SerializationStrategyTest.class.getSimpleName();
    public Strategy strategy = Strategy.PORTABLE;

    public int itemCount = 1000000;
    public int recordsPerUnique = 10000;

    public double getByStringIndexProb = 1;
    public double getByIntIndexProb = 0;

    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();

    private IMap<String, DomainObject> map;
    private Set<String> uniqueStrings;

    @Setup
    public void setUp(TestContext testContext) {
        map = testContext.getTargetInstance().getMap(basename);
        uniqueStrings = testContext.getTargetInstance().getSet(basename);

        operationSelectorBuilder.addOperation(Operation.GET_BY_STRING_INDEX, getByStringIndexProb)
                .addOperation(Operation.GET_BY_INT_INDEX, getByIntIndexProb)
                .addDefaultOperation(Operation.GET_BY_KEY);
    }

    @Warmup(global = true)
    public void initDataLoad() {
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
        Set<String> stringsSet = new HashSet<String>(uniqueStringsCount);
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

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        private String[] localUniqueStrings;

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void beforeRun() throws Exception {
            localUniqueStrings = uniqueStrings.toArray(new String[uniqueStrings.size()]);
            super.beforeRun();
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            switch (operation) {
                case GET_BY_KEY:
                    throw new UnsupportedOperationException("Not implemented yet");
                case GET_BY_INT_INDEX:
                    throw new UnsupportedOperationException("Not implemented yet");
                case GET_BY_STRING_INDEX:
                    String string = getUniqueString();
                    Predicate predicate = Predicates.equal("stringVal", string);
                    Set<Map.Entry<String, DomainObject>> entries = map.entrySet(predicate);
                    THROTTLING_LOGGER.log(Level.INFO, "GetByStringIndex: " + entries.size() + " entries");
                    break;
                default:
                    throw new IllegalArgumentException("Unknown Operation: " + operation);
            }
        }

        public String getUniqueString() {
            int i = randomInt(localUniqueStrings.length);
            return localUniqueStrings[i];
        }
    }
}
