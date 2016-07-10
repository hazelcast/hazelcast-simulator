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
package com.hazelcast.simulator.tests.jmx;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;

public class PartitionServiceMBeanTest extends AbstractTest {

    private enum Operation {
        IS_LOCAL_MEMBER_SAFE,
        IS_CLUSTER_SAFE
    }

    // properties
    public int minNumberOfMembers = 0;
    public double isLocalMemberSafeProb = 0;

    private MBeanServer mBeanServer;
    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private ObjectName objectName;

    @Setup
    public void setUp() throws Exception {
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
        this.objectName = new ObjectName("com.hazelcast:instance=" + targetInstance.getName()
                + ",name=" + targetInstance.getName() + ",type=HazelcastInstance.PartitionServiceMBean");

        operationSelectorBuilder.addOperation(Operation.IS_LOCAL_MEMBER_SAFE, isLocalMemberSafeProb)
                .addDefaultOperation(Operation.IS_CLUSTER_SAFE);
    }

    @Warmup(global = false)
    public void warmup() {
        waitClusterSize(logger, targetInstance, minNumberOfMembers);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorkerWithMultipleProbes<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation, Probe probe) throws Exception {
            long started;
            switch (operation) {
                case IS_CLUSTER_SAFE:
                    started = System.nanoTime();
                    mBeanServer.getAttribute(objectName, "isClusterSafe");
                    probe.done(started);
                    break;
                case IS_LOCAL_MEMBER_SAFE:
                    started = System.nanoTime();
                    mBeanServer.getAttribute(objectName, "isLocalMemberSafe");
                    probe.done(started);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    @Teardown
    public void tearDown() {
        logger.info(getOperationCountInformation(targetInstance));
    }
}
