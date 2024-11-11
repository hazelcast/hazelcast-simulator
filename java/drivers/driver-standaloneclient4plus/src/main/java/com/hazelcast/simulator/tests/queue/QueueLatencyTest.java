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
package com.hazelcast.simulator.tests.queue;

import com.hazelcast.collection.IQueue;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.StopException;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;

import static org.junit.Assert.assertEquals;

public class QueueLatencyTest extends HazelcastTest {

    private IAtomicLong produced;
    private IAtomicLong consumed;
    private IQueue<Long> workQueue;

    @Setup
    public void setup() {
        produced = getAtomicLong(name + ":Produced");
        consumed = getAtomicLong(name + ":Consumed");
        workQueue = targetInstance.getQueue(name + ":WorkQueue");
    }

    @TimeStep(executionGroup = "producer")
    public void produce(ProducerState state) {
        workQueue.offer(0L);
        state.produced++;
    }

    @AfterRun(executionGroup = "producer")
    public void afterRun(ProducerState state) {
        produced.addAndGet(state.produced);
        workQueue.add(-1L);
    }

    public class ProducerState extends BaseThreadState {
        long produced;
    }

    @TimeStep(executionGroup = "consumer")
    public void consume(ConsumerState state) {
        Long item = workQueue.poll();

        if (item != null) {
            if (item.equals(-1L)) {
                workQueue.add(item);
                throw new StopException();
            }

            state.consumed++;
        }
    }

    @AfterRun(executionGroup = "consumer")
    public void afterRun(ConsumerState state) {
        consumed.addAndGet(state.consumed);
    }

    public class ConsumerState extends BaseThreadState {
        long consumed;
    }

    @Verify
    public void verify() {
        long expected = produced.get();
        long actual = consumed.get();
        assertEquals(expected, actual);
    }

    @Teardown
    public void teardown() {
        produced.destroy();
        workQueue.destroy();
        consumed.destroy();
    }
}
