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
package com.hazelcast.simulator.worker.testcontainer;

import java.util.concurrent.Callable;

/**
 * A RunStrategy encapsulates the logic for the 3 different types of test running approaches:
 * <ol>
 * <li>{@link com.hazelcast.simulator.test.annotations.Run}</li>
 * <li>{@link com.hazelcast.simulator.test.annotations.RunWithWorker}</li>
 * <li>{@link com.hazelcast.simulator.test.annotations.TimeStep}</li>
 * </ol>
 */
abstract class RunStrategy {

    private volatile boolean running;
    private volatile long startedMillis;

    public abstract Callable getRunCallable();

     /**
     * Returns the number of iterations of all the executions. Value is 0 if it isn't tracked, or the information is only
     * available through Probes.
     *
     * @return the number of operations.
     */
    public long iterations() {
        return 0;
    }

    /**
     * Checks if the run strategy is running. This is true in case of warmup and actual running.
     *
     * This method is thread-safe.
     *
     * @return true if running, false otherwise.
     */
    public final boolean isRunning() {
        return running;
    }

    /**
     * Notifies the RunStrategy it has started running.
     */
    final void onRunStarted() {
        running = true;
        startedMillis = System.currentTimeMillis();
    }

    /**
     * Notifies the RunStrategy that it has completed running.
     */
    final void onRunCompleted() {
        running = false;
    }

    /**
     * Returns the timestap when the test started running. As long as the test has not started, the returned value is 0.
     *
     * This method is thread-safe.
     *
     * @return the started timestamp.
     */
    final long getStartedMillis() {
        return startedMillis;
    }
}
