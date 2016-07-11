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

import com.hazelcast.simulator.test.PropertyBinding;
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
    private final Class timeStepRunnerClass;
    private final TimeStepModel timeStepModel;
    private final ThreadSpawner spawner;
    private volatile TimeStepRunner[] runners;
    private final PropertyBinding propertyBinding;

    public TimeStepRunStrategy(TestContainer testContainer) {
        this.propertyBinding = testContainer.getPropertyBinding();
        propertyBinding.inject(this);

        this.testContainer = testContainer;
        this.testContext = testContainer.getTestContext();
        this.testInstance = testContainer.getTestInstance();

        TimeStepRunnerCodeGenerator codeGenerator = new TimeStepRunnerCodeGenerator();
        this.timeStepModel = new TimeStepModel(testInstance.getClass(), propertyBinding);

        this.timeStepRunnerClass = codeGenerator.compile(
                 timeStepModel,
                propertyBinding.getMetronomeClass(),
                propertyBinding.getProbeClass());

        this.spawner = new ThreadSpawner(testContext.getTestId());
    }

    @Override
    public long iterations() {
        TimeStepRunner[] runners = this.runners;
        long iterations = 0;
        if (runners != null) {
            for (TimeStepRunner runner : runners) {
                iterations += runner.iteration();
            }
        }
        return iterations;
    }

    @Override
    public Object call() throws Exception {
        try {
            LOGGER.info(format("Spawning %d worker threads for test %s", threadCount, testContext.getTestId()));

            if (threadCount <= 0) {
                return null;
            }

            onRunStarted();
            spawnTimeStepRunners();
            spawner.awaitCompletion();
        } finally {
            onRunCompleted();
        }

        return null;
    }

    private void spawnTimeStepRunners() throws Exception {
        TimeStepRunner[] runners = new TimeStepRunner[threadCount];
        Constructor<TimeStepRunner> constructor = timeStepRunnerClass.getConstructor(Object.class, TimeStepModel.class);
        for (int i = 0; i < threadCount; i++) {
            TimeStepRunner runner = constructor.newInstance(testInstance, timeStepModel);
            propertyBinding.inject(runner);
            spawner.spawn(runner);
            runners[i] = runner;
        }
        this.runners = runners;
    }
}
