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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.worker.selector.OperationSelector;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;

import java.util.Random;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;

/**
 * Abstract worker class which is returned by {@link com.hazelcast.simulator.test.annotations.RunWithWorker} annotated test
 * methods.
 *
 * Implicitly logs and measures performance. The related properties can be overwritten with the properties of the test.
 * The Operation counter is automatically increased after each {@link #timeStep(Enum)} call.
 *
 * @param <O> Type of Enum used by the {@link com.hazelcast.simulator.worker.selector.OperationSelector}
 */
@SuppressWarnings("unused")
public abstract class AbstractWorker<O extends Enum<O>> implements IWorker {

    protected static final ILogger LOGGER = Logger.getLogger(AbstractWorker.class);

    final OperationSelector<O> selector;

    @InjectTestContext
    TestContext testContext;
    @InjectProbe(useForThroughput = true)
    Probe workerProbe;

    private final Random random = new Random();

    private long iteration;
    private boolean isWorkerStopped;

    public AbstractWorker(OperationSelectorBuilder<O> operationSelectorBuilder) {
        this.selector = operationSelectorBuilder.build();
    }

    /**
     * This constructor is just for child classes who also override the {@link #run()} method.
     */
    AbstractWorker() {
        this.selector = null;
    }

    @Override
    public final void run() {
        try {
            doRun();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected void doRun() throws Exception {
        beforeRun();

        while (!testContext.isStopped() && !isWorkerStopped) {
            long started = System.nanoTime();
            timeStep(selector.select());
            workerProbe.recordValue(System.nanoTime() - started);

            increaseIteration();
        }

        afterRun();
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
     * Override this method if you need to execute code on each worker before {@link #run()} is called.
     */
    protected void beforeRun() throws Exception {
    }

    /**
     * This method is called for each iteration of {@link #run()}.
     *
     * Won't be called if an error occurs in {@link #beforeRun()}.
     *
     * @param operation The selected operation for this iteration
     */
    protected abstract void timeStep(O operation) throws Exception;

    /**
     * Override this method if you need to execute code on each worker after {@link #run()} is called.
     *
     * Won't be called if an error occurs in {@link #beforeRun()} or {@link #timeStep(Enum)}.
     */
    protected void afterRun() throws Exception {
    }

    /**
     * Override this method if you need to execute code once after all workers have finished their run phase.
     *
     * @see IWorker
     */
    public void afterCompletion() throws Exception {
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
    protected Random getRandom() {
        return random;
    }

    /**
     * Returns the iteration count of the worker.
     *
     * @return iteration count
     */
    protected long getIteration() {
        return iteration;
    }

    void increaseIteration() {
        iteration++;
    }

    boolean isWorkerStopped() {
        return isWorkerStopped;
    }
}
