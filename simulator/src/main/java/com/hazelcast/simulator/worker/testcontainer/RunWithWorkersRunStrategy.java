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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import static com.hazelcast.simulator.utils.CommonUtils.rethrow;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static java.lang.String.format;

/**
 * A {@link RunStrategy} for tests with a method annotated with {@link com.hazelcast.simulator.test.annotations.RunWithWorker}.
 *
 * @deprecated and is likely to be removed in Simulator 0.10.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class RunWithWorkersRunStrategy extends RunStrategy {

    static final int DEFAULT_THREAD_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(RunWithWorkersRunStrategy.class);

    private int threadCount;
    private final TestContainer testContainer;
    private final Method runWithWorkersMethod;
    private final TestContext testContext;
    private final Object testInstance;

    public RunWithWorkersRunStrategy(TestContainer testContainer, Method runWithWorkersMethod) {
        PropertyBinding binding = testContainer.getPropertyBinding();

        this.threadCount = binding.loadAsInt("threadCount", DEFAULT_THREAD_COUNT);
        this.testContainer = testContainer;
        this.testContext = testContainer.getTestContext();
        this.testInstance = testContainer.getTestInstance();
        this.runWithWorkersMethod = runWithWorkersMethod;
    }

    @Override
    public Callable getRunCallable() {
        return new Callable() {
            @Override
            public Object call() throws Exception {
                try {
                    LOGGER.info(format("Spawning %d worker threads for test %s", threadCount, testContext.getTestId()));
                    if (threadCount <= 0) {
                        return null;
                    }

                    // create the workers.
                    IWorker[] workers = createWorkers();

                    onRunStarted();

                    // spawn workers-threads and wait for completion
                    ThreadSpawner spawner = spawnThreads(workers);
                    spawner.awaitCompletion();

                    // call the afterCompletion() method on a single instance of the worker
                    workers[0].afterCompletion();

                    return null;
                } finally {
                    onRunCompleted();
                }
            }
        };
    }

    private ThreadSpawner spawnThreads(IWorker[] workers) throws Exception {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        for (IWorker worker : workers) {
            spawner.spawn(new WorkerTask(worker));
        }
        return spawner;
    }

    private IWorker[] createWorkers() throws Exception {
        IWorker[] workers = new IWorker[threadCount];
        for (int i = 0; i < threadCount; i++) {
            IWorker worker = invokeMethod(testInstance, runWithWorkersMethod);
            testContainer.getPropertyBinding().bind(worker);
            workers[i] = worker;
        }
        return workers;
    }

    private static class WorkerTask implements Runnable {

        private final IWorker worker;

        WorkerTask(IWorker worker) {
            this.worker = worker;
        }

        @Override
        public void run() {
            try {
                worker.beforeRun();
                worker.run();
                worker.afterRun();
            } catch (Exception e) {
                throw rethrow(e);
            }
        }
    }
}
