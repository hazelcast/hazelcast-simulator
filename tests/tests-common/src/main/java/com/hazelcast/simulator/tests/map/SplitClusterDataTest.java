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
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;

public class SplitClusterDataTest extends AbstractTest {

    public int maxItems = 10000;
    public int clusterSize = -1;
    public int splitClusterSize = -1;

    private IMap<Object, Object> map;

    @Setup
    public void setup() {
        map = targetInstance.getMap(name);

        if (clusterSize == -1 || splitClusterSize == -1) {
            throw new IllegalStateException("priorities: clusterSize == -1 Or splitClusterSize == -1");
        }
    }

    @Prepare(global = true)
    public void prepare() {
        waitClusterSize(logger, targetInstance, clusterSize);

        for (int i = 0; i < maxItems; i++) {
            map.put(i, i);
        }
    }

    @TimeStep
    public void timeStep() {
        sleepSeconds(1);
    }

    @Verify(global = false)
    public void verify() {
        logger.info(name + ": cluster size =" + targetInstance.getCluster().getMembers().size());
        logger.info(name + ": map size =" + map.size());

        if (targetInstance.getCluster().getMembers().size() == splitClusterSize) {
            logger.info(name + ": check again cluster =" + targetInstance.getCluster().getMembers().size());
        } else {
            logger.info(name + ": check again cluster =" + targetInstance.getCluster().getMembers().size());

            int max = 0;
            while (map.size() != maxItems) {
                sleepMillis(1000);
                if (max++ == 60) {
                    break;
                }
            }

            assertEquals("data loss ", map.size(), maxItems);
            logger.info(name + "verify OK ");
        }
    }
}
