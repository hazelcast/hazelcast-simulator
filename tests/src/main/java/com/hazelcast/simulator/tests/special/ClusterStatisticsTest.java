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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitionService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.IWorker;
import com.hazelcast.simulator.worker.tasks.NoOperationWorker;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.logPartitionStatistics;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

public class ClusterStatisticsTest {

    private static final ILogger LOGGER = Logger.getLogger(ClusterStatisticsTest.class);

    // properties
    public String basename = ClusterStatisticsTest.class.getSimpleName();
    public int isClusterSafeRetries = 10;

    private HazelcastInstance hazelcastInstance;
    private PartitionService partitionService;
    private IMap<Object, Integer> map;

    @Setup
    public void setUp(TestContext testContext) {
        this.hazelcastInstance = testContext.getTargetInstance();
        this.partitionService = hazelcastInstance.getPartitionService();
        this.map = hazelcastInstance.getMap(basename);
    }

    @Warmup
    public void warmup() {
        if (!isMemberNode(hazelcastInstance)) {
            return;
        }

        int retry = 0;
        while (!partitionService.isClusterSafe() && retry++ < isClusterSafeRetries) {
            LOGGER.info(basename + ": isClusterSafe() " + partitionService.isClusterSafe());
            sleepSeconds(1);
        }
        LOGGER.info(basename + ": isClusterSafe() " + partitionService.isClusterSafe());
        LOGGER.info(basename + ": isLocalMemberSafe() " + partitionService.isLocalMemberSafe());
        LOGGER.info(basename + ": getCluster().getMembers().size() " + hazelcastInstance.getCluster().getMembers().size());

        logPartitionStatistics(LOGGER, basename, map, false);
    }

    @RunWithWorker
    public IWorker createWorker() {
        return new NoOperationWorker();
    }
}
