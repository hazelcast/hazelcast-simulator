/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;

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

        while (!testContext.isStopped() && !isWorkerStopped) {
            long started = System.nanoTime();
            try {
                timeStep();
            } catch (Exception e) {
                throw rethrow(e);
            }
            workerProbe.recordValue(System.nanoTime() - started);

            increaseIteration();
        }

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
    protected abstract void timeStep() throws Exception;
}
