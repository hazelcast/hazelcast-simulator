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
package com.hazelcast.simulator.tests.special;

import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.logPartitionStatistics;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class ClusterStatisticsTest extends HazelcastTest {

    // properties
    public int isClusterSafeRetries = 10;

    private PartitionService partitionService;
    private IMap<Object, Integer> map;

    @Setup
    public void setUp() {
        this.partitionService = targetInstance.getPartitionService();
        this.map = targetInstance.getMap(name);
    }

    @Prepare
    public void prepare() {
        if (!isMemberNode(targetInstance)) {
            return;
        }

        int retry = 0;
        while (!partitionService.isClusterSafe() && retry++ < isClusterSafeRetries) {
            logger.info(name + ": isClusterSafe() " + partitionService.isClusterSafe());
            sleepSeconds(1);
        }
        logger.info(name + ": isClusterSafe() " + partitionService.isClusterSafe());
        logger.info(name + ": isLocalMemberSafe() " + partitionService.isLocalMemberSafe());
        logger.info(name + ": getCluster().getMembers().size() " + targetInstance.getCluster().getMembers().size());

        logPartitionStatistics(logger, name, map, false);
    }

    @TimeStep
    public void timeStep() {
        testContext.stop();
    }
}
