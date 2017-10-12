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

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.worker.metronome.Metronome;

import java.util.Random;

/**
 * @deprecated is likely to be removed in Simulator 0.10.
 */
abstract class VeryAbstractWorker implements IWorker {

    private final Random random = new Random();

    @InjectTestContext
    private TestContext testContext;
    @InjectMetronome
    private Metronome workerMetronome;

    private boolean isWorkerStopped;
    private long iteration;

    VeryAbstractWorker() {
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public void afterCompletion() throws Exception {
    }

    /**
     * Returns the {@link TestContext} for this worker.
     *
     * @return the {@link TestContext}
     */
    protected final TestContext getTestContext() {
        return testContext;
    }

    /**
     * Returns the {@link Metronome} instance of this worker.
     *
     * @return the {@link Metronome} instance
     */
    protected final Metronome getWorkerMetronome() {
        return workerMetronome;
    }

    /**
     * Checks if the local worker is stopped, regardless of the {@link TestContext} stopped status.
     *
     * @return {@code true} if the local worker is stopped, {@code false} otherwise
     */
    protected final boolean isWorkerStopped() {
        return isWorkerStopped;
    }

    /**
     * Stops the local worker, regardless of the {@link TestContext} stopped status.
     *
     * Calling this method will not affect the {@link TestContext} or other workers. It will just stop the local worker.
     */
    protected final void stopWorker() {
        isWorkerStopped = true;
    }

    /**
     * Stops the {@link TestContext}.
     *
     * Calling this method will affect all workers of the {@link TestContext}.
     */
    protected final void stopTestContext() {
        testContext.stop();
    }

    /**
     * Returns the test ID from the {@link TestContext}.
     *
     * @return the test ID
     */
    protected final String getTestId() {
        return testContext.getTestId();
    }

    /**
     * Calls {@link Random#nextInt()} on an internal Random instance.
     *
     * @return the next pseudo random, uniformly distributed {@code int} value from this random number generator's sequence
     */
    protected final int randomInt() {
        return random.nextInt();
    }

    /**
     * Calls {@link Random#nextInt(int)} on an internal Random instance.
     *
     * @param upperBond the bound on the random number to be returned.  Must be
     *                  positive.
     * @return the next pseudo random, uniformly distributed {@code int} value between {@code 0} (inclusive) and {@code n}
     * (exclusive) from this random number generator's sequence
     */
    protected final int randomInt(int upperBond) {
        return random.nextInt(upperBond);
    }

    /**
     * Returns the inner {@link Random} instance to call methods which are not implemented.
     *
     * @return the {@link Random} instance of the worker
     */
    protected final Random getRandom() {
        return random;
    }

    /**
     * Returns the iteration count of the worker.
     *
     * @return iteration count
     */
    protected final long getIteration() {
        return iteration;
    }

    /**
     * Increases the iteration count of the worker.
     */
    protected final void increaseIteration() {
        iteration++;
    }
}
