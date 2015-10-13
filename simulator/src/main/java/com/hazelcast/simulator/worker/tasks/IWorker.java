package com.hazelcast.simulator.worker.tasks;

/**
 * Interface for workers who are returned by {@link com.hazelcast.simulator.test.annotations.RunWithWorker} annotated test
 * methods.
 *
 * Your implementation will get the following (optional) fields injected by {@link com.hazelcast.simulator.worker.TestContainer}:
 * {@link com.hazelcast.simulator.test.TestContext TestContext} testContext;
 * {@link com.hazelcast.simulator.probes.Probe Probe} workerProbe;
 * <code>long</code> logFrequency;
 */
public interface IWorker extends Runnable {

    /**
     * Implement this method if you need to execute code once after all workers have finished their run phase.
     *
     * Will always be called by the {@link com.hazelcast.simulator.worker.TestContainer}, regardless of errors in the run phase.
     * Will be executed after {@link com.hazelcast.simulator.utils.ThreadSpawner#awaitCompletion()} on a single worker instance.
     */
    void afterCompletion();
}
