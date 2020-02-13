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

package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.FailureType;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.coordinator.operations.FailureOperation;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.util.HashMap;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static java.lang.String.format;

/**
 * The CoordinatorRunMonolith takes care of the good old simple to use simulator where you automatically create some workers,
 * run a test and shut it down. None of that fancy interactive stuff.
 */
class CoordinatorRunMonolith {

    private static final int WAIT_FOR_WORKER_FAILURE_RETRY_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(CoordinatorRunMonolith.class);

    private final Coordinator coordinator;
    private final FailureCollector failureCollector;
    private final TestPhase lastTestPhaseToSync;

    CoordinatorRunMonolith(Coordinator coordinator, CoordinatorParameters coordinatorParameters) {
        this.coordinator = coordinator;
        this.failureCollector = coordinator.getFailureCollector();
        this.lastTestPhaseToSync = coordinatorParameters.getLastTestPhaseToSync();
    }

    public void init(DeploymentPlan deploymentPlan) throws Exception {
        logConfiguration(deploymentPlan);

        try {
            coordinator.createStartWorkersTask(deploymentPlan.getWorkerDeployment(), new HashMap<>()).run();
        } catch (Exception e) {
            failureCollector.notify(
                    new FailureOperation("Failed to create worker", FailureType.WORKER_CREATE_ERROR, null, null, null));
            throw e;
        }
    }

    public boolean run(TestSuite testSuite) throws Exception {
        if (testSuite.getDurationSeconds() == 0) {
            LOGGER.info("Test suite runs without time-limit, it will complete when it decides it's ready or CTRL-C is pressed");
        }

        try {
            coordinator.createRunTestSuiteTask(testSuite).run();

        } catch (CommandLineExitException e) {
            for (int i = 0; i < WAIT_FOR_WORKER_FAILURE_RETRY_COUNT && failureCollector.getFailureCount() == 0; i++) {
                sleepSeconds(1);
            }
            throw e;
        }
        return !failureCollector.hasCriticalFailure();
    }

    private void logConfiguration(DeploymentPlan deploymentPlan) {
        LOGGER.info(format("Total number of Hazelcast member workers: %s", deploymentPlan.getMemberWorkerCount()));
        LOGGER.info(format("Total number of Hazelcast client workers: %s", deploymentPlan.getClientWorkerCount()));
        LOGGER.info(format("Last TestPhase to sync: %s", lastTestPhaseToSync));
    }
}
