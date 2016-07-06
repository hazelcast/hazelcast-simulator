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
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;

import static java.lang.String.format;

/**
 * A {@link RunStrategy} used for tests containing methods with {@link com.hazelcast.simulator.test.annotations.TimeStep}
 * annotation.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class TimeStepRunStrategy extends RunStrategy {

    public static final int DEFAULT_THREAD_COUNT = 10;
    private static final Logger LOGGER = Logger.getLogger(TimeStepRunStrategy.class);

    // properties
    public int threadCount = DEFAULT_THREAD_COUNT;

    private final TestContainer testContainer;
    private final TestContext testContext;
    private final Object testInstance;
    private final Class timestepTaskClass;
    private final TimeStepModel timeStepModel;
    private final ThreadSpawner spawner;
    private volatile TimeStepTask[] tasks;

    public TimeStepRunStrategy(TestContainer testContainer) {
        testContainer.getDependencyInjector().inject(this);

        this.testContainer = testContainer;
        this.testContext = testContainer.getTestContext();
        this.testInstance = testContainer.getTestInstance();

        TimeStepTaskCodeGenerator codeGenerator = new TimeStepTaskCodeGenerator();
        this.timeStepModel = new TimeStepModel(testInstance.getClass());

        this.timestepTaskClass = codeGenerator.compile(
                 timeStepModel,
                testContainer.getDependencyInjector().getMetronomeClass(),
                testContainer.getDependencyInjector().getProbeClass());

        this.spawner = new ThreadSpawner(testContext.getTestId());
    }

    @Override
    public long iterations() {
        TimeStepTask[] workers = this.tasks;
        long iterations = 0;
        if (workers != null) {
            for (TimeStepTask worker : workers) {
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
            onRunStarted();

            // spawn tasks and wait for completion
            spawnTasks();

            spawner.awaitCompletion();
        } finally {
            onRunCompleted();
        }

        return null;
    }

    private void spawnTasks() throws Exception {
        TimeStepTask[] tasks = new TimeStepTask[threadCount];
        Constructor<TimeStepTask> constructor = timestepTaskClass.getConstructor(Object.class, TimeStepModel.class);
        for (int i = 0; i < threadCount; i++) {
            final TimeStepTask task = constructor.newInstance(testInstance, timeStepModel);

            testContainer.getDependencyInjector().inject(task);

//               if (operationProbes != null) {
//                ((IMultipleProbesWorker) worker).setProbeMap(operationProbes);
//            }

            spawner.spawn(task);
            tasks[i] = task;
        }
        this.tasks = tasks;
    }
}
