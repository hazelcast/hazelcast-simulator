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

import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.map.helpers.MapMaxSizeOperationCounter;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import static com.hazelcast.config.MaxSizeConfig.MaxSizePolicy.PER_NODE;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

/**
 * This tests runs {@link IMap#put(Object, Object)} and {@link IMap#get(Object)} operations on a map, which is configured with
 * {@link com.hazelcast.config.MaxSizeConfig.MaxSizePolicy#PER_NODE}.
 *
 * With some probability distribution we are doing put, putAsync, get and verification operations on the map.
 * We verify during the test and at the end that the map has not exceeded its max configured size.
 */
public class MapMaxSizeTest {

    private enum MapOperation {
        PUT,
        GET,
        CHECK_SIZE
    }

    private enum MapPutOperation {
        PUT_SYNC,
        PUT_ASYNC
    }

    private static final ILogger LOGGER = Logger.getLogger(MapMaxSizeTest.class);

    // properties
    public String basename = MapMaxSizeTest.class.getSimpleName();

    public double putProb = 0.5;
    public double getProb = 0.4;
    public double checkProb = 0.1;

    public double putUsingAsyncProb = 0.2;
    public boolean assertMaxSize = true;

    private final OperationSelectorBuilder<MapOperation> mapOperationSelectorBuilder
            = new OperationSelectorBuilder<MapOperation>();
    private final OperationSelectorBuilder<MapPutOperation> mapPutOperationSelectorBuilder
            = new OperationSelectorBuilder<MapPutOperation>();

    private HazelcastInstance targetInstance;
    private IMap<Object, Object> map;
    private IList<MapMaxSizeOperationCounter> operationCounterList;
    private int maxSizePerNode;
    private int keyCount = Integer.MAX_VALUE;


    @Setup
    public void setUp(TestContext testContext) {
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);
        operationCounterList = targetInstance.getList(basename + "OperationCounter");

        mapOperationSelectorBuilder
                .addOperation(MapOperation.PUT, putProb)
                .addOperation(MapOperation.GET, getProb)
                .addOperation(MapOperation.CHECK_SIZE, checkProb);

        mapPutOperationSelectorBuilder
                .addOperation(MapPutOperation.PUT_ASYNC, putUsingAsyncProb)
                .addDefaultOperation(MapPutOperation.PUT_SYNC);


        if (isMemberNode(targetInstance)) {

            MaxSizeConfig maxSizeConfig = targetInstance.getConfig().getMapConfig(basename).getMaxSizeConfig();
            maxSizePerNode = maxSizeConfig.getSize();

            LOGGER.info("MapSizeConfig of " + basename + ": " + maxSizeConfig);
        }
    }

    @Verify(global = true)
    public void globalVerify() {
        MapMaxSizeOperationCounter total = new MapMaxSizeOperationCounter();
        for (MapMaxSizeOperationCounter operationCounter : operationCounterList) {
            total.add(operationCounter);
        }
        LOGGER.info(format("Operation counters from %s: %s", basename, total));
    }


    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<MapOperation> {
        private final MapMaxSizeOperationCounter operationCounter = new MapMaxSizeOperationCounter();
        private final OperationSelector<MapPutOperation> mapPutSelector = mapPutOperationSelectorBuilder.build();

        public Worker() {
            super(mapOperationSelectorBuilder);
        }

        @Override
        public void timeStep(MapOperation operation) {
            final int key = randomInt(keyCount);

            switch (operation) {
                case PUT:
                    final Object value = randomInt();
                    switch (mapPutSelector.select()) {
                        case PUT_SYNC:
                            map.put(key, value);
                            operationCounter.put++;
                            break;
                        case PUT_ASYNC:
                            map.putAsync(key, value);
                            operationCounter.putAsync++;
                            break;
                        default:
                            throw new UnsupportedOperationException();
                    }

                    break;
                case GET:
                    map.get(key);
                    operationCounter.get++;
                    break;
                case CHECK_SIZE:

                default:
                    throw new UnsupportedOperationException();
            }
        }

        @Override
        protected void afterRun() {
            operationCounterList.add(operationCounter);
        }
    }
}
