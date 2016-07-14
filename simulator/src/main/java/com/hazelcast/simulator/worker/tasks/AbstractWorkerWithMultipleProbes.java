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
import com.hazelcast.simulator.test.PropertyBinding;
import com.hazelcast.simulator.test.PropertyBindingAware;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

import java.util.Set;

import static org.apache.commons.lang3.text.WordUtils.capitalizeFully;

/**
 * Version of {@link AbstractWorker} with an individual {@link Probe} per operation.
 * <p>
 * This worker provides a {@link #timeStep(Enum, Probe)} method with the operation specific {@link Probe} as additional parameter.
 * This can be used to make a finer selection of the measured code block.
 *
 * @param <O> Type of {@link Enum} used by the {@link com.hazelcast.simulator.worker.selector.OperationSelector}
 */
@Deprecated
public abstract class AbstractWorkerWithMultipleProbes<O extends Enum<O>>
        extends VeryAbstractWorker
        implements PropertyBindingAware {

    private final OperationSelectorBuilder<O> operationSelectorBuilder;
    private final OperationSelector<O> operationSelector;

    private Probe[] workerProbes;

    public AbstractWorkerWithMultipleProbes(OperationSelectorBuilder<O> operationSelectorBuilder) {
        this.operationSelectorBuilder = operationSelectorBuilder;
        this.operationSelector = operationSelectorBuilder.build();
    }

    @Override
    public void bind(PropertyBinding binding) {
        Set<O> operations = operationSelectorBuilder.getOperations();
        workerProbes = new Probe[operations.size()];
        for (Enum operation : operations) {
            String probeName = capitalizeFully(operation.name(), '_').replace("_", "") + "Probe";
            Probe probe = binding.getOrCreateProbe(probeName, false);
            workerProbes[operation.ordinal()] = probe;
        }
    }

    @Override
    public final void run() throws Exception {
        final TestContext testContext = getTestContext();
        final Metronome metronome = getWorkerMetronome();
        final OperationSelector<O> selector = operationSelector;
        final Probe[] probes = workerProbes;

        while (!testContext.isStopped() && !isWorkerStopped()) {
            metronome.waitForNext();
            O op = selector.select();
            Probe probe = probes[op.ordinal()];

            long started = System.nanoTime();
            timeStep(op, probe);
            probe.recordValue(System.nanoTime() - started);
            increaseIteration();
        }
    }

    /**
     * This method is called for each iteration of {@link #run()}.
     * <p>
     * Won't be called if an error occurs in {@link #beforeRun()}.
     *
     * @param operation The selected operation for this iteration
     * @param probe     The individual {@link Probe} for this operation
     * @throws Exception is allowed to throw exceptions which are automatically reported as failure
     */
    protected abstract void timeStep(O operation, Probe probe) throws Exception;
}
