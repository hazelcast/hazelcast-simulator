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

/**
 * Interface for workers who are returned by {@link com.hazelcast.simulator.test.annotations.RunWithWorker} annotated test
 * methods.
 *
 * Your implementation will get the following (optional) fields injected by {@link com.hazelcast.simulator.worker.TestContainer}:
 * {@link com.hazelcast.simulator.test.TestContext TestContext} testContext;
 * {@link com.hazelcast.simulator.probes.Probe Probe} workerProbe;
 * <code>long</code> logFrequency;
 */
public interface IWorker extends Runnable {

    /**
     * Implement this method if you need to execute code once after all workers have finished their run phase.
     *
     * Will always be called by the {@link com.hazelcast.simulator.worker.TestContainer}, regardless of errors in the run phase.
     * Will be executed after {@link com.hazelcast.simulator.utils.ThreadSpawner#awaitCompletion()} on a single worker instance.
     */
    void afterCompletion();
}
