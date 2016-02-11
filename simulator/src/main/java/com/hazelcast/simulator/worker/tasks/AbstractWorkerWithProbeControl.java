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

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

/**
 * Version of {@link AbstractWorker} which allows full control over the built-in {@link Probe}.
 *
 * This worker provides a {@link #timeStep(Enum, Probe)} method with the built-in {@link Probe} as additional parameter.
 * This can be used to make a finer selection of the measured code block.
 *
 * @param <O> Type of {@link Enum} used by the {@link com.hazelcast.simulator.worker.selector.OperationSelector}
 */
public abstract class AbstractWorkerWithProbeControl<O extends Enum<O>> extends AbstractWorker<O> {

    public AbstractWorkerWithProbeControl(OperationSelectorBuilder<O> operationSelectorBuilder) {
        super(operationSelectorBuilder);
    }

    @Override
    protected void doRun() throws Exception {
        timeStep(getRandomOperation(), getWorkerProbe());

        increaseIteration();
    }

    /**
     * Fake implementation of abstract method, should not be used.
     *
     * @param operation ignored
     */
    @Override
    protected final void timeStep(O operation) {
        throw new UnsupportedOperationException();
    }

    /**
     * This method is called for each iteration of {@link #run()}.
     *
     * Won't be called if an error occurs in {@link #beforeRun()}.
     *
     * @param operation The selected operation for this iteration
     * @param probe     The built-in {@link Probe}
     */
    protected abstract void timeStep(O operation, Probe probe) throws Exception;
}
