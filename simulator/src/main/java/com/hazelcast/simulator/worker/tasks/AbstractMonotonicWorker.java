package com.hazelcast.simulator.worker.tasks;

/**
 * Monotonic version of {@link AbstractWorker}.
 *
 * This worker provides no {@link com.hazelcast.simulator.worker.selector.OperationSelector}, just a simple {@link #timeStep()}
 * method without parameters.
 */
public abstract class AbstractMonotonicWorker extends AbstractWorker {

    @Override
    public final void run() {
        beforeRun();

        while (!testContext.isStopped()) {
            timeStep();

            increaseIteration();
        }
        operationCount.addAndGet(iteration % performanceUpdateFrequency);

        afterRun();
    }

    /**
     * Fake implementation of abstract method, should not be used.
     *
     * @param operation ignored
     */
    @Override
    protected final void timeStep(Enum operation) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is called for each iteration of {@link #run()}.
     *
     * Won't be called if an error occurs in {@link #beforeRun()}.
     */
    protected abstract void timeStep();
}
