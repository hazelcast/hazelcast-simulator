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
package com.hazelcast.simulator.tests.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.waitClusterSize;

public class PartitionServiceMBeanTest {

    private static final ILogger LOGGER = Logger.getLogger(PartitionServiceMBeanTest.class);

    private enum Operation {
        IsLocalMemberSafe,
        IsClusterSafe
    }

    // properties
    public int minNumberOfMembers = 0;
    public double isLocalMemberSafeProb = 0;

    // probes
    public Probe isLocalMemberSafeProbe;
    public Probe isClusterSafe;

    private MBeanServer mBeanServer;
    private final OperationSelectorBuilder<Operation> operationSelectorBuilder = new OperationSelectorBuilder<Operation>();
    private HazelcastInstance targetInstance;
    private ObjectName name;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        targetInstance = testContext.getTargetInstance();

        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
        this.name = new ObjectName("com.hazelcast:instance=" + targetInstance.getName()
                + ",name=" + targetInstance.getName() + ",type=HazelcastInstance.PartitionServiceMBean");

        operationSelectorBuilder.addOperation(Operation.IsLocalMemberSafe, isLocalMemberSafeProb)
                .addDefaultOperation(Operation.IsClusterSafe);
    }

    @Teardown
    public void tearDown() {
        LOGGER.info(getOperationCountInformation(targetInstance));
    }

    @Warmup(global = false)
    public void warmup() {
        waitClusterSize(LOGGER, targetInstance, minNumberOfMembers);
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorker<Operation> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            switch (operation) {
                case IsClusterSafe:
                    isClusterSafe.started();
                    mBeanServer.getAttribute(name, "isClusterSafe");
                    isClusterSafe.done();
                    break;
                case IsLocalMemberSafe:
                    isLocalMemberSafeProbe.started();
                    mBeanServer.getAttribute(name, "isLocalMemberSafe");
                    isLocalMemberSafeProbe.done();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PartitionServiceMBeanTest test = new PartitionServiceMBeanTest();
        new TestRunner<PartitionServiceMBeanTest>(test).run();
    }
}
