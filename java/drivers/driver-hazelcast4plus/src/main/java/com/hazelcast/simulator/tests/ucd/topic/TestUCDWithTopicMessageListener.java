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

import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.tests.helpers.KeyLocality;
import com.hazelcast.simulator.tests.helpers.KeyUtils;
import com.hazelcast.simulator.tests.ucd.UCDTest;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.MessageListener;

import java.util.concurrent.Phaser;

public class TestUCDWithTopicMessageListener extends UCDTest {
    /**
     * If you push the {@link ITopic} to breaking point, messages are dropped and throughput is no longer properly measurable
     */
    int concurrency = 1000;

    @Run
    public void run() throws ReflectiveOperationException {
        ThreadSpawner spawner = new ThreadSpawner(name);

        for (Object key : KeyUtils.generateIntKeys(concurrency, KeyLocality.LOCAL, targetInstance)) {
            spawner.spawn(new Task(key));
        }

        spawner.awaitCompletion();
    }

    private class Task implements Runnable {
        private final Object key;
        private final ITopic<Object> topic;
        private final LatencyProbe probe = testContext.getLatencyProbe("foo");
        private final Phaser phaser = new Phaser(1);

        private Task(Object key) throws ReflectiveOperationException {
            this.key = key;
            topic = targetInstance.getTopic(name + key);

            topic.addMessageListener(
                    (MessageListener<Object>) udf.getDeclaredConstructor(phaser.getClass()).newInstance(phaser));
        }

        /** Measure how long it takes for a message to be sent & received */
        @Override
        public void run() {
            while (!testContext.isStopped()) {
                phaser.register();
                long startNanos = System.nanoTime();

                topic.publish(key);
                phaser.arriveAndAwaitAdvance();

                probe.recordValue(System.nanoTime() - startNanos);
            }
        }
    }
}