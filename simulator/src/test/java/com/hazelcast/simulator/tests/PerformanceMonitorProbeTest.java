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

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;

import java.util.concurrent.CountDownLatch;

import static com.hazelcast.simulator.utils.CommonUtils.await;

public class PerformanceMonitorProbeTest {

    private CountDownLatch testStartedLatch = new CountDownLatch(1);
    private CountDownLatch stopTestLatch = new CountDownLatch(1);
    private Worker worker = new Worker();

    public void recordValue(long latencyNanos) {
        await(testStartedLatch);
        worker.workerProbe.recordValue(latencyNanos);
    }

    public void stopTest() {
        stopTestLatch.countDown();
    }

    @RunWithWorker
    public Worker createWorker() {
        return worker;
    }

    private class Worker implements IWorker {

        @InjectProbe(useForThroughput = true)
        Probe workerProbe;

        @Override
        public void run() {
            testStartedLatch.countDown();
            await(stopTestLatch);
        }

        @Override
        public void beforeRun() {
        }

        @Override
        public void afterRun() {
        }

        @Override
        public void afterCompletion() {
        }
    }
}
