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
        public void afterCompletion() {
        }
    }
}
