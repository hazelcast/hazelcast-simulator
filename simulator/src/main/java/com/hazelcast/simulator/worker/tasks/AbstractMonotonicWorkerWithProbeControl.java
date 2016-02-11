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

/**
 * Monotonic version of {@link AbstractWorker} which allows full control over the built-in {@link Probe}.
 *
 * This worker provides no {@link com.hazelcast.simulator.worker.selector.OperationSelector}, but a {@link #timeStep(Probe)}
 * method with the built-in {@link Probe} as parameter. This can be used to make a finer selection of the measured code block.
 */
public abstract class AbstractMonotonicWorkerWithProbeControl extends AbstractWorker {

    @Override
    public final void doRun() throws Exception {
        timeStep(getWorkerProbe());

        increaseIteration();
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
     *
     * @param probe The built-in {@link Probe}
     */
    protected abstract void timeStep(Probe probe) throws Exception;
}
