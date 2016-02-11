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
package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

/**
 * Asynchronous version of {@link AbstractWorker}.
 *
 * The operation counter is automatically increased after each call of {@link ExecutionCallback#onResponse}.
 * The {@link Throwable} is automatically reported after each call of {@link ExecutionCallback#onFailure(Throwable)}
 *
 * @param <O> Type of {@link Enum} used by the {@link com.hazelcast.simulator.worker.selector.OperationSelector}
 * @param <V> Type of {@link ExecutionCallback}
 */
public abstract class AbstractAsyncWorker<O extends Enum<O>, V> extends AbstractWorker<O> implements ExecutionCallback<V> {

    public AbstractAsyncWorker(OperationSelectorBuilder<O> operationSelectorBuilder) {
        super(operationSelectorBuilder);
    }

    @Override
    public final void doRun() throws Exception {
        timeStep(getRandomOperation());
    }

    @Override
    public final void onResponse(V response) {
        try {
            increaseIteration();
        } finally {
            handleResponse(response);
        }
    }

    @Override
    public final void onFailure(Throwable t) {
        try {
            ExceptionReporter.report(getTestId(), t);
        } finally {
            handleFailure(t);
        }
    }

    /**
     * Implement this method if you need to execute code on each worker after the iteration has been increased in
     * {@link ExecutionCallback#onResponse(Object)}.
     *
     * @param response the result of the successful execution
     */
    protected abstract void handleResponse(V response);

    /**
     * Implement this method if you need to execute code on each worker after the throwable has been reported in
     * {@link ExecutionCallback#onFailure(Throwable)}.
     *
     * @param t the exception that is thrown
     */
    protected abstract void handleFailure(Throwable t);
}
