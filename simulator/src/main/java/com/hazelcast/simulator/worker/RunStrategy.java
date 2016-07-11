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
package com.hazelcast.simulator.worker;

import java.util.concurrent.Callable;

/**
 * A RunStrategy encapsulates the logic for the 3 different types of test running approaches:
 * - @run annotation
 * - @runwithworker workers
 * - @timestep method
 */
public abstract class RunStrategy implements Callable {

    private volatile boolean running;
    private volatile long startedTimeStamp;

    /**
     * Returns the number  of iterations.
     *
     * @return
     */
    public abstract long iterations();

    public final boolean isRunning() {
        return running;
    }

    protected final void onRunStarted() {
        running = true;
        startedTimeStamp = System.currentTimeMillis();
    }

    protected final void onRunCompleted() {
        running = false;
    }

    public final long getStartedTimestamp() {
        return startedTimeStamp;
    }
}
