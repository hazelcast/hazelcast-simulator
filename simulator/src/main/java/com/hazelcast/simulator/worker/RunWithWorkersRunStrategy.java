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

import com.hazelcast.simulator.test.TestContainer;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;

import static com.hazelcast.simulator.utils.ReflectionUtils.invokeMethod;
import static java.lang.String.format;

/**
 * A {@link RunStrategy} for tests with a method annotated with {@link com.hazelcast.simulator.test.annotations.RunWithWorker}.
 */
public class RunWithWorkersRunStrategy extends RunStrategy {
    private static final Logger LOGGER = Logger.getLogger(RunWithWorkersRunStrategy.class);

    // properties
    public int threadCount;

    private final TestContainer testContainer;
    private final Method runWithWorkersMethod;
    private final TestContext testContext;
    private final Object testInstance;
    private volatile IWorker[] workers;

    public RunWithWorkersRunStrategy(TestContainer testContainer, Method runWithWorkersMethod) {
        testContainer.getDependencyInjector().inject(this);

        this.testContainer = testContainer;
        this.testContext = testContainer.getTestContext();
        this.testInstance = testContainer.getTestInstance();
        this.runWithWorkersMethod = runWithWorkersMethod;
    }

    @Override
    public long iterations() {
        IWorker[] workers = this.workers;
        long iterations = 0;
        if (workers != null) {
            for (IWorker worker : workers) {
                iterations += worker.iteration();
            }
        }
        return iterations;
    }

    @Override
    public Object call() throws Exception {
        try {
            LOGGER.info(format("Spawning %d worker threads for test %s",
                    threadCount, testContext.getTestId()));

            if (threadCount <= 0) {
                return null;
            }

            // create instance to get the class of the IWorker implementation
            //IWorker workerInstance = invokeMethod(testInstance, runWithWorkersMethod);
            //Class<? extends IWorker> workerClass = workerInstance.getClass();
            ///    Map<Enum, Probe> operationProbeMap = createOperationProbeMap(workerClass, workerInstance);

            onRunStarted();
            // spawn workers and wait for completion
            IWorker worker = runWorkers();

            // call the afterCompletion() method on a single instance of the worker
            worker.afterCompletion();
        } finally {
            onRunCompleted();
        }

        return null;
    }

    private IWorker runWorkers() throws Exception {
        IWorker firstWorker = null;
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());
        IWorker[] workers = new IWorker[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final IWorker worker = invokeMethod(testInstance, runWithWorkersMethod);
            if (firstWorker == null) {
                firstWorker = worker;
            }

            testContainer.getDependencyInjector().inject(worker);
//            if (operationProbes != null) {
//                ((IMultipleProbesWorker) worker).setProbeMap(operationProbes);
//            }

            spawner.spawn(new WorkerTask(worker));
            workers[i] = worker;
        }
        this.workers = workers;
        spawner.awaitCompletion();
        return firstWorker;
    }
}
