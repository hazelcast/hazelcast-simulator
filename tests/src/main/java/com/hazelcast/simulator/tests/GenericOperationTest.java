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
package com.hazelcast.simulator.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorkerWithMultipleProbes;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationService;

public class GenericOperationTest {

    private static final ILogger LOGGER = Logger.getLogger(GenericOperationTest.class);

    private enum PrioritySelector {
        PRIORITY,
        NORMAL
    }

    // properties
    public double priorityProb = 0.1;
    public int delayNs = 100 * 1000;

    private OperationService operationService;
    private Address[] memberAddresses;

    private final OperationSelectorBuilder<PrioritySelector> operationSelectorBuilder
            = new OperationSelectorBuilder<PrioritySelector>();

    private HazelcastInstance instance;

    @Setup
    public void setUp(TestContext testContext) {
        instance = testContext.getTargetInstance();
        operationService = getOperationService(instance);

        operationSelectorBuilder
                .addOperation(PrioritySelector.PRIORITY, priorityProb)
                .addDefaultOperation(PrioritySelector.NORMAL);
    }

    @Warmup
    public void warmup() {
        Set<Member> memberSet = instance.getCluster().getMembers();
        memberAddresses = new Address[memberSet.size()];

        int i = 0;
        for (Member member : memberSet) {
            memberAddresses[i++] = new Address(member.getSocketAddress());
        }
    }

    @Teardown
    public void tearDown() {
        LOGGER.info(getOperationCountInformation(instance));
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractWorkerWithMultipleProbes<PrioritySelector> {

        public Worker() {
            super(operationSelectorBuilder);
        }

        @Override
        protected void timeStep(PrioritySelector operationSelector, Probe probe) {
            Address address = randomAddress();

            switch (operationSelector) {
                case PRIORITY:
                    invokeOperation(new GenericPriorityOperation(delayNs), address, probe);
                    break;
                case NORMAL:
                    invokeOperation(new GenericOperation(delayNs), address, probe);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private Address randomAddress() {
            return memberAddresses[randomInt(memberAddresses.length)];
        }

        private void invokeOperation(Operation operation, Address address, Probe probe) {
            long started = System.nanoTime();
            InternalCompletableFuture future = operationService.invokeOnTarget(null, operation, address);
            future.getSafely();
            probe.done(started);
        }
    }

    private static class GenericOperation extends AbstractOperation {

        private int delayNanos;

        GenericOperation(int delayNanos) {
            this();
            this.delayNanos = delayNanos;
        }

        GenericOperation() {
            setPartitionId(-1);
        }

        @Override
        public void run() throws Exception {
            if (delayNanos > 0) {
                Random random = new Random();
                LockSupport.parkNanos(random.nextInt(delayNanos));
            }
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(delayNanos);
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            delayNanos = in.readInt();
        }
    }

    private static class GenericPriorityOperation extends GenericOperation implements UrgentSystemOperation {

        @SuppressWarnings("unused")
        public GenericPriorityOperation() {
        }

        GenericPriorityOperation(int delayNanos) {
            super(delayNanos);
        }
    }

    public static void main(String[] args) throws Exception {
        GenericOperationTest test = new GenericOperationTest();
        new TestRunner<GenericOperationTest>(test).withDuration(10).run();
    }
}
