package com.hazelcast.stabilizer.worker.tasks;

/**
 * Monotonic version of {@link AbstractWorkerTask}.
 * <p/>
 * This worker provides no {@link com.hazelcast.stabilizer.worker.selector.OperationSelector}, just a simple {@link #timeStep()}
 * method without parameters.
 */
public abstract class AbstractMonotonicWorkerTask extends AbstractWorkerTask {

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
     * <p/>
     * Won't be called if an error occurs in {@link #beforeRun()}.
     */
    protected abstract void timeStep();
}
