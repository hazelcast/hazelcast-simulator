package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

/**
 * Asynchronous version of {@link AbstractWorker}.
 *
 * The operation counter is automatically increased after call of {@link com.hazelcast.core.ExecutionCallback#onResponse}.
 *
 * @param <O> Type of Enum used by the {@link com.hazelcast.simulator.worker.selector.OperationSelector}
 * @param <V> Type of {@link com.hazelcast.core.ExecutionCallback}
 */
public abstract class AbstractAsyncWorker<O extends Enum<O>, V> extends AbstractWorker<O>
        implements ExecutionCallback<V> {

    public AbstractAsyncWorker(OperationSelectorBuilder<O> operationSelectorBuilder) {
        super(operationSelectorBuilder);
    }

    @Override
    public final void run() {
        while (!testContext.isStopped()) {
            timeStep(selector.select());
        }
        operationCount.addAndGet(iteration % performanceUpdateFrequency);
    }

    @Override
    public final void onResponse(V response) {
        increaseIteration();

        handleResponse(response);
    }

    @Override
    public final void onFailure(Throwable t) {
        ExceptionReporter.report(testContext.getTestId(), t);

        handleFailure(t);
    }

    /**
     * Will be called on {@link ExecutionCallback#onResponse(Object)} after the iteration has been increased.
     *
     * @param response the result of the successful execution
     */
    protected abstract void handleResponse(V response);

    /**
     * Will be called on {@link ExecutionCallback#onFailure(Throwable)} after the throwable has been reported.
     *
     * @param t the exception that is thrown
     */
    protected abstract void handleFailure(Throwable t);
}
