package com.hazelcast.stabilizer.worker.tasks;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.stabilizer.utils.ExceptionReporter;
import com.hazelcast.stabilizer.worker.selector.OperationSelectorBuilder;

/**
 * Asynchronous version of {@link AbstractWorkerTask}.
 * <p/>
 * The operation counter is automatically increased after call of {@link com.hazelcast.core.ExecutionCallback#onResponse}.
 *
 * @param <O> Type of Enum used by the {@link com.hazelcast.stabilizer.worker.selector.OperationSelector}
 * @param <V> Type of {@link com.hazelcast.core.ExecutionCallback}
 */
public abstract class AsyncAbstractWorkerTask<O extends Enum<O>, V> extends AbstractWorkerTask<O>
        implements ExecutionCallback<V> {

    public AsyncAbstractWorkerTask(OperationSelectorBuilder<O> operationSelectorBuilder) {
        super(operationSelectorBuilder);
    }

    @Override
    public void run() {
        while (!testContext.isStopped()) {
            doRun(selector.select());
        }
        operationCount.addAndGet(iteration % performanceUpdateFrequency);
    }

    @Override
    public void onResponse(V response) {
        increaseIteration();

        handleResponse(response);
    }

    @Override
    public void onFailure(Throwable t) {
        ExceptionReporter.report(testContext.getTestId(), t);

        handleFailure(t);
    }

    protected abstract void handleResponse(V response);

    protected abstract void handleFailure(Throwable t);
}
