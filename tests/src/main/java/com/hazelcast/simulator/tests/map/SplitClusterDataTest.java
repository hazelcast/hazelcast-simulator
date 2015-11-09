/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThreadSpawner;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

public class SplitClusterDataTest {

    private static final ILogger LOGGER = Logger.getLogger(SplitClusterDataTest.class);

    public String basename = SplitClusterDataTest.class.getSimpleName();
    public int maxItems = 10000;
    public int clusterSize = -1;
    public int splitClusterSize = -1;

    private TestContext testContext;
    private HazelcastInstance targetInstance;
    private IMap<Object, Object> map;

    @Setup
    public void setup(TestContext testContext) {
        this.testContext = testContext;
        targetInstance = testContext.getTargetInstance();
        map = targetInstance.getMap(basename);

        if (clusterSize == -1 || splitClusterSize == -1) {
            throw new IllegalStateException("priorities: clusterSize == -1 Or splitClusterSize == -1");
        }
    }

    @Warmup(global = true)
    public void warmup() {
        waitClusterSize(LOGGER, targetInstance, clusterSize);

        for (int i = 0; i < maxItems; i++) {
            map.put(i, i);
        }
    }

    @Run
    public void run() {
        ThreadSpawner spawner = new ThreadSpawner(basename);
        spawner.spawn(new Worker());
        spawner.awaitCompletion();
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            while (!testContext.isStopped()) {
                sleepSeconds(1);
            }
        }
    }

    @Verify(global = false)
    public void verify() {
        LOGGER.info(basename + ": cluster size =" + targetInstance.getCluster().getMembers().size());
        LOGGER.info(basename + ": map size =" + map.size());

        if (targetInstance.getCluster().getMembers().size() == splitClusterSize) {
            LOGGER.info(basename + ": check again cluster =" + targetInstance.getCluster().getMembers().size());
        } else {
            LOGGER.info(basename + ": check again cluster =" + targetInstance.getCluster().getMembers().size());

            int max = 0;
            while (map.size() != maxItems) {
                sleepMillis(1000);
                if (max++ == 60) {
                    break;
                }
            }

            assertEquals("data loss ", map.size(), maxItems);
            LOGGER.info(basename + "verify OK ");
        }
    }
}
