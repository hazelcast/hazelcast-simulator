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

/**
 * Interface for workers who are returned by {@link com.hazelcast.simulator.test.annotations.RunWithWorker} annotated test
 * methods.
 *
 * The reason this class is prefixed with I, is that the name 'Worker' can be used in the actual tests.
 *
 * The {@link com.hazelcast.simulator.worker.testcontainer.TestContainer} supports the following injections for
 * your implementation:
 * {@link com.hazelcast.simulator.test.annotations.InjectTestContext}
 * {@link com.hazelcast.simulator.test.annotations.InjectHazelcastInstance}
 * {@link com.hazelcast.simulator.test.annotations.InjectProbe}
 *
 * @deprecated is likely to be removed in Simulator 0.10.
 */
public interface IWorker {

    /**
     * Name for the default {@link com.hazelcast.simulator.probes.Probe} which will be injected to the worker by the
     * {@link com.hazelcast.simulator.worker.testcontainer.TestContainer}.
     */
    String DEFAULT_WORKER_PROBE_NAME = "workerProbe";

    /**
     * Override this method if you need to execute code on each worker before {@link #run()} is called.
     *
     * @throws Exception is allowed to throw exceptions which are automatically reported as failure
     */
    void beforeRun() throws Exception;

    /**
     * Runs the actual worker logic.
     *
     * @throws Exception is allowed to throw exceptions which are automatically reported as failure
     */
    void run() throws Exception;

    /**
     * Override this method if you need to execute code on each worker after {@link #run()} is called.
     *
     * Won't be called if an error occurs in {@link #beforeRun()} or {@link #run()}.
     *
     * @throws Exception is allowed to throw exceptions which are automatically reported as failure
     */
    void afterRun() throws Exception;

    /**
     * Implement this method if you need to execute code once after all workers have finished their run phase.
     *
     * Will always be called by the {@link com.hazelcast.simulator.worker.testcontainer.TestContainer}, regardless
     * of errors in the run phase. Will be executed after {@link com.hazelcast.simulator.utils.ThreadSpawner#awaitCompletion()}
     * on a single worker instance.
     *
     * @throws Exception is allowed to throw exceptions which are automatically reported as failure
     */
    void afterCompletion() throws Exception;
}
