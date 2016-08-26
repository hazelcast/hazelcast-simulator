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

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
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

    private final TestContext testContext;
    private final Object testInstance;
    private final TimeStepModel timeStepModel;
    private final PropertyBinding propertyBinding;
    private volatile TimeStepRunner[] runners;
    private final Map<String, Class> runnerClassMap = new HashMap<String, Class>();
    private final Map<String, Integer> threadCountMap = new HashMap<String, Integer>();
    private int totalThreadCount;

    public TimeStepRunStrategy(TestContainer testContainer) {
        this.propertyBinding = testContainer.getPropertyBinding();
        this.propertyBinding.bind(this);

        this.testContext = testContainer.getTestContext();
        this.testInstance = testContainer.getTestInstance();
        this.timeStepModel = new TimeStepModel(testInstance.getClass(), propertyBinding);

        for (String executionGroup : timeStepModel.getExecutionGroups()) {
            Class runnerClass = new TimeStepRunnerCodeGenerator().compile(
                    testContainer.getTestCase().getId(),
                    executionGroup,
                    timeStepModel,
                    propertyBinding.getMetronomeClass(),
                    propertyBinding.getProbeClass());
            runnerClassMap.put(executionGroup, runnerClass);

            int threadCount = loadThreadCount(executionGroup);
            totalThreadCount += threadCount;
            threadCountMap.put(executionGroup, threadCount);
        }
    }

    private int loadThreadCount(String group) {
        String threadCountName = group.equals("") ? "threadCount" : group + "ThreadCount";
        String value = propertyBinding.loadProperty(threadCountName);
        if (value == null) {
            return DEFAULT_THREAD_COUNT;
        } else {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalTestException("property " + threadCountName + " with value '" + value + "' is not an Integer");
            }
        }
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
                    LOGGER.info(format("Spawning %d worker threads for running %s", totalThreadCount, testContext.getTestId()));

                    if (totalThreadCount <= 0) {
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
                    LOGGER.info(format("Spawning %d worker threads for warmup %s", totalThreadCount, testContext.getTestId()));

                    if (totalThreadCount <= 0) {
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
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());

        for (TimeStepRunner runner : runners) {
            String executionGroup = runner.executionGroup;
            String name = testContext.getTestId();
            if (!executionGroup.equals("")) {
                name += "-" + executionGroup;
            }
            name += warmup ? "-warmup" : "-run";
            name += "Thread";
            spawner.spawn(name, runner);
        }

        return spawner;
    }

    @SuppressWarnings("unchecked")
    private TimeStepRunner[] createRunners() throws Exception {
        TimeStepRunner[] returnRunners = new TimeStepRunner[totalThreadCount];

        int k = 0;
        for (String executionGroup : timeStepModel.getExecutionGroups()) {
            Class runnerClass = runnerClassMap.get(executionGroup);
            Constructor<TimeStepRunner> constructor = runnerClass
                    .getConstructor(testInstance.getClass(), TimeStepModel.class, String.class);

            for (int thread = 0; thread < threadCountMap.get(executionGroup); thread++) {
                TimeStepRunner runner = constructor.newInstance(testInstance, timeStepModel, executionGroup);
                propertyBinding.bind(runner);
                returnRunners[k] = runner;
                k++;
            }
        }

        return returnRunners;
    }
}