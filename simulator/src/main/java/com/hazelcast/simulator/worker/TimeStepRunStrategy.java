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
import java.util.concurrent.Callable;

import static java.lang.String.format;

/**
 * A {@link RunStrategy} used for tests containing methods with {@link com.hazelcast.simulator.test.annotations.TimeStep}
 * annotation.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class TimeStepRunStrategy extends RunStrategy {

    private static final int DEFAULT_THREAD_COUNT = 10;

    private static final Logger LOGGER = Logger.getLogger(TimeStepRunStrategy.class);

    // properties
    public int threadCount = DEFAULT_THREAD_COUNT;

    private final TestContext testContext;
    private final Object testInstance;
    private final Class runnerClass;
    private final TimeStepModel timeStepModel;
    private final PropertyBinding propertyBinding;
    private volatile TimeStepRunner[] runners;

    public TimeStepRunStrategy(TestContainer testContainer) {
        this.propertyBinding = testContainer.getPropertyBinding();
        this.propertyBinding.bind(this);

        this.testContext = testContainer.getTestContext();
        this.testInstance = testContainer.getTestInstance();
        this.timeStepModel = new TimeStepModel(testInstance.getClass(), propertyBinding);
        this.runnerClass = new TimeStepRunnerCodeGenerator().compile(
                testContainer.getTestCase().getId(),
                timeStepModel,
                propertyBinding.getMetronomeClass(),
                propertyBinding.getProbeClass());
    }

    @Override
    public long iterations() {
        TimeStepRunner[] localRunners = runners;
        long iterations = 0;
        if (localRunners != null) {
            for (TimeStepRunner runner : localRunners) {
                iterations += runner.iteration();
            }
        }
        return iterations;
    }

    @Override
    public Callable getRunCallable() {
        return new Callable() {
            @Override
            public Object call() throws Exception {
                try {
                    LOGGER.info(format("Spawning %d worker threads for running %s", threadCount, testContext.getTestId()));

                    if (threadCount <= 0) {
                        return null;
                    }
                    runners = createRunners();
                    onRunStarted();
                    ThreadSpawner spawner = spawnThreads(runners, false);
                    spawner.awaitCompletion();

                    return null;
                } finally {
                    onRunCompleted();
                }
            }
        };
    }

    @Override
    public Callable getWarmupCallable() {
        return new Callable() {
            @Override
            public Object call() throws Exception {
                try {
                    LOGGER.info(format("Spawning %d worker threads for warmup %s", threadCount, testContext.getTestId()));

                    if (threadCount <= 0) {
                        return null;
                    }
                    runners = createRunners();
                    onRunStarted();
                    ThreadSpawner spawner = spawnThreads(runners, true);
                    spawner.awaitCompletion();
                    return null;
                } finally {
                    onRunCompleted();
                }
            }
        };
    }

    private ThreadSpawner spawnThreads(TimeStepRunner[] runners, boolean warmup) {
        String identifier = (warmup ? "warmup-" : "run-") + testContext.getTestId();
        ThreadSpawner spawner = new ThreadSpawner(identifier);

        for (TimeStepRunner runner : runners) {
            spawner.spawn(runner);
        }

        return spawner;
    }

    @SuppressWarnings("unchecked")
    private TimeStepRunner[] createRunners() throws Exception {
        TimeStepRunner[] tmpRunners = new TimeStepRunner[threadCount];
        Constructor<TimeStepRunner> constructor = runnerClass.getConstructor(testInstance.getClass(), TimeStepModel.class);
        for (int i = 0; i < threadCount; i++) {
            TimeStepRunner runner = constructor.newInstance(testInstance, timeStepModel);
            propertyBinding.bind(runner);
            tmpRunners[i] = runner;
        }
        return tmpRunners;
    }
}
