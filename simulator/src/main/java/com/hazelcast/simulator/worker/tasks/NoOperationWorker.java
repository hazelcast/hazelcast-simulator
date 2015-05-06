package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.test.annotations.Performance;

/**
 * Dummy worker which does nothing.
 *
 * Can be used on nodes which should not interact with the cluster.
 */
public final class NoOperationWorker extends AbstractMonotonicWorker {

    @Override
    protected void beforeRun() {
        testContext.stop();
    }

    @Override
    protected void timeStep() {
    }

    @Performance
    public long getOperationCount() {
        return 0;
    }
}
