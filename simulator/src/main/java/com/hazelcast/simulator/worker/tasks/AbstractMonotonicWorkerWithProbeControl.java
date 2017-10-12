/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.worker.metronome.Metronome;

/**
 * Monotonic version of {@link AbstractWorker} which allows full control over the built-in {@link Probe}.
 *
 * This worker provides no {@link com.hazelcast.simulator.worker.selector.OperationSelector}, but a {@link #timeStep(Probe)}
 * method with the built-in {@link Probe} as parameter. This can be used to make a finer selection of the measured code block.
 *
 * @deprecated is likely to be removed in Simulator 0.10.
 */
public abstract class AbstractMonotonicWorkerWithProbeControl extends VeryAbstractWorker {

    @InjectProbe(name = IWorker.DEFAULT_WORKER_PROBE_NAME, useForThroughput = true)
    private Probe workerProbe;

    @Override
    public final void run() throws Exception {
        final TestContext testContext = getTestContext();
        final Metronome metronome = getWorkerMetronome();
        final Probe probe = workerProbe;

        while (!testContext.isStopped() && !isWorkerStopped()) {
            metronome.waitForNext();
            timeStep(probe);
            increaseIteration();
        }
    }

    /**
     * This method is called for each iteration of {@link #run()}.
     *
     * Won't be called if an error occurs in {@link #beforeRun()}.
     *
     * @param probe The built-in {@link Probe}
     *
     * @throws Exception is allowed to throw exceptions which are automatically reported as failure
     */
    protected abstract void timeStep(Probe probe) throws Exception;
}
