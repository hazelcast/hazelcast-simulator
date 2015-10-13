package com.hazelcast.simulator.worker.tasks;

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
}
