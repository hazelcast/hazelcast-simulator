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
package com.hazelcast.simulator.tests.topic;

import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

import java.util.concurrent.CountDownLatch;

public class ReliableTopicTestRUN extends HazelcastTest {
    public int concurrency = 1;

    // TODO Multiple topics
    private ITopic<Long> topic;

    @Setup
    public void setUp() {
        topic = targetInstance.getReliableTopic(KeyUtils.generateStringKey(1, KeyLocality.SHARED, targetInstance));
    }

    @Run
    public void run() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(concurrency);

        for (int k = 0; k < concurrency; k++) {
            GetTask obj = new GetTask(latch);
            topic.addMessageListener(obj);
            obj.run();
        }

        latch.await();
    }

    private class GetTask implements MessageListener<Long> {
        private final CountDownLatch latch;
        private final LatencyProbe latencyProbe;

        public GetTask(CountDownLatch latch) {
            latencyProbe = testContext.getLatencyProbe(name);
            this.latch = latch;
        }

        @Override
        public void onMessage(Message<Long> message) {
            latencyProbe.recordValue(System.nanoTime() - message.getMessageObject());
            run();
        }

        public void run() {
            if (testContext.isStopped()) {
                latch.countDown();
            } else {
                topic.publish(System.nanoTime());
            }
        }
    }
}