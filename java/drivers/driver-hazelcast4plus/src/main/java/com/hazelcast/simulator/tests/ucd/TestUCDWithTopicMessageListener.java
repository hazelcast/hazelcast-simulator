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
package com.hazelcast.simulator.tests.ucd;

import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class TestUCDWithTopicMessageListener extends HazelcastTest {
    private ITopic<Object> topic;

    private CompletableFuture<Object> future;
    private UUID listenerRegistration;

    @Setup
    public void setUp() {
        topic = targetInstance.getReliableTopic(KeyUtils.generateStringKey(1, KeyLocality.SHARED, targetInstance));
    }

    @BeforeRun
    public void beforeRun() {
        future = new CompletableFuture<>();
        listenerRegistration = topic.addMessageListener(new CompletableMessageListener(future));
    }

    @TimeStep
    public void timeStep(LatencyProbe latencyProbe) {
        topic.publish(Void.TYPE);
        future.join();
        latencyProbe.done(System.nanoTime());
    }

    @AfterRun
    public void afterRun() {
        topic.removeMessageListener(listenerRegistration);
    }

    private static class CompletableMessageListener implements MessageListener<Object> {
        private final CompletableFuture<Object> future;

        public CompletableMessageListener(CompletableFuture<Object> future) {
            this.future = future;
        }

        @Override
        public void onMessage(Message<Object> message) {
            future.complete(message.getMessageObject());
        }
    }
}