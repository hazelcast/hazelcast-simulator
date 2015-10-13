package com.hazelcast.simulator.tests;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.SimulatorProbe;
import com.hazelcast.simulator.utils.EmptyStatement;
import com.hazelcast.simulator.worker.tasks.IWorker;

import java.util.concurrent.CountDownLatch;

public class PerformanceMonitorProbeTest {

    private CountDownLatch testStartedLatch = new CountDownLatch(1);
    private CountDownLatch stopTestLatch = new CountDownLatch(1);
    private Worker worker = new Worker();

    @RunWithWorker
    public Worker createWorker() {
        return worker;
    }

    public void recordValue(long latencyNanos) throws Exception {
        testStartedLatch.await();
        worker.workerProbe.recordValue(latencyNanos);
    }

    public void stopTest() {
        stopTestLatch.countDown();
    }

    private class Worker implements IWorker {

        @SimulatorProbe(useForThroughput = true)
        Probe workerProbe;

        @Override
        public void run() {
            testStartedLatch.countDown();
            try {
                stopTestLatch.await();
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }

        @Override
        public void afterCompletion() {
        }
    }
}
