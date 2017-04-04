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

import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.UrgentSystemOperation;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationCountInformation;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.getOperationService;

public class GenericOperationTest extends HazelcastTest {

    // properties
    public int delayNs = 100 * 1000;

    private OperationService operationService;
    private Address[] memberAddresses;

    @Setup
    public void setUp() {
        operationService = getOperationService(targetInstance);
    }

    @Prepare
    public void prepare() {
        Set<Member> memberSet = targetInstance.getCluster().getMembers();
        memberAddresses = new Address[memberSet.size()];

        int i = 0;
        for (Member member : memberSet) {
            memberAddresses[i++] = new Address(member.getSocketAddress());
        }
    }

    @TimeStep(prob = 0.1)
    public void priority(ThreadState state) throws ExecutionException, InterruptedException {
        Address address = state.randomAddress();
        GenericPriorityOperation op = new GenericPriorityOperation(delayNs);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, op, address);
        f.get();
    }

    @TimeStep(prob = -1)
    public void normal(ThreadState state) throws ExecutionException, InterruptedException {
        Address address = state.randomAddress();
        GenericOperation op = new GenericOperation(delayNs);
        InternalCompletableFuture f = operationService.invokeOnTarget(null, op, address);
        f.get();
    }

    public class ThreadState extends BaseThreadState {
        private Address randomAddress() {
            return memberAddresses[randomInt(memberAddresses.length)];
        }
    }

    private static class GenericOperation extends Operation {

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

    @Teardown
    public void tearDown() {
        logger.info(getOperationCountInformation(targetInstance));
    }


}
