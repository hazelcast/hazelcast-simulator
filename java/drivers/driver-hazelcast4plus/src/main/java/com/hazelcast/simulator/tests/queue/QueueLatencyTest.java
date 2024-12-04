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
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

public class QueueLatencyTest extends HazelcastTest {

    private IQueue<Long> queue;

    @Setup
    public void setup() {
        queue = targetInstance.getQueue(name + "-queue");
    }

    @TimeStep(executionGroup = "producer")
    public void produce() {
        queue.offer(0L);
    }

    @TimeStep(executionGroup = "consumer")
    public void consume() {
        queue.poll();
    }

    public void teardown() {
        queue.destroy();
    }
}
