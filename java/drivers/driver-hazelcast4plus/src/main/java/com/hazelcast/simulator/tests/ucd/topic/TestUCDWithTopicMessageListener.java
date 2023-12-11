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
package com.hazelcast.simulator.tests.ucd.topic;

import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.tests.ucd.UCDTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.MessageListener;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class TestUCDWithTopicMessageListener extends UCDTest {
    private ITopic<Object> topic;

    private CompletableFuture<Object> future;
    private UUID listenerRegistration;

    @Override
    @Setup
    public void setUp() throws ReflectiveOperationException {
        super.setUp();
        topic = targetInstance.getReliableTopic(name);
    }

    @BeforeRun
    public void beforeRun() throws ReflectiveOperationException {
        future = new CompletableFuture<>();
        listenerRegistration = topic.addMessageListener(
                (MessageListener<Object>) udf.getDeclaredConstructor(future.getClass()).newInstance(future));
    }

    /** Measure how long it takes for a message to be sent & received */
    @TimeStep
    public void timeStep() {
        topic.publish(Void.TYPE);
        future.join();
    }

    @AfterRun
    public void afterRun() {
        topic.removeMessageListener(listenerRegistration);
    }
}