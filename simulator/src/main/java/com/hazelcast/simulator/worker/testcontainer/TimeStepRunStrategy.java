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

import static com.hazelcast.simulator.worker.testcontainer.PropertyBinding.toPropertyName;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link RunStrategy} used for tests containing methods with {@link com.hazelcast.simulator.test.annotations.TimeStep}
 * annotation.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
class TimeStepRunStrategy extends RunStrategy {

    private static final int DEFAULT_THREAD_COUNT = 10;
    private static final int DEFAULT_LOG_FREQUENCY = 0;
    private static final int DEFAULT_LOG_RATE_MS = 0;

    private static final Logger LOGGER = Logger.getLogger(TimeStepRunStrategy.class);

    private final TestContext testContext;
    private final Object testInstance;
    private final TimeStepModel timeStepModel;
    private final PropertyBinding binding;
    private volatile TimeStepRunner[] runners;
    private final Map<String, MetronomeSupplier> metronomeSettingsMap = new HashMap<>();
    private final Map<String, Class> runnerClassMap = new HashMap<>();
    private final Map<String, Integer> threadCountMap = new HashMap<>();
    private final Map<String, Long> runIterationMap = new HashMap<>();
    private int totalThreadCount;

    TimeStepRunStrategy(TestContainer testContainer) {
        this.binding = testContainer.getPropertyBinding();
        this.testContext = testContainer.getTestContext();
        this.testInstance = testContainer.getTestInstance();
        this.timeStepModel = new TimeStepModel(testInstance.getClass(), binding);

        for (String executionGroup : timeStepModel.getExecutionGroups()) {
            int threadCount = binding.loadAsInt(toPropertyName(executionGroup, "threadCount"), DEFAULT_THREAD_COUNT);
            totalThreadCount += threadCount;
            threadCountMap.put(executionGroup, threadCount);

            MetronomeSupplier metronomeConstructor = new MetronomeSupplier(executionGroup, binding, threadCount);
            metronomeSettingsMap.put(executionGroup, metronomeConstructor);

            LOGGER.info(format("executionGroup [%s] using interval: %s class=%s",
                    executionGroup, metronomeConstructor.getIntervalNanos(), metronomeConstructor.getMetronomeClass().getName()));

            long logFrequency = binding.loadAsLong(toPropertyName(executionGroup, "logFrequency"), DEFAULT_LOG_FREQUENCY);
            long logRateMs = binding.loadAsLong(toPropertyName(executionGroup, "logRateMs"), DEFAULT_LOG_RATE_MS);

            long iterations = binding.loadAsLong(toPropertyName(executionGroup, "iterations"), 0);
            runIterationMap.put(executionGroup, iterations);

            Class runnerClass = new TimeStepRunnerCodeGenerator().compile(
                    testContainer.getTestCase().getId(),
                    executionGroup,
                    timeStepModel,
                    metronomeConstructor.getMetronomeClass(),
                    binding.getProbeClass(),
                    logFrequency,
                    logRateMs,
                    iterations > 0);

            runnerClassMap.put(executionGroup, runnerClass);
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
        return () -> {
            try {
                LOGGER.info(format("Spawning %d worker threads for running %s", totalThreadCount, testContext.getTestId()));

                if (totalThreadCount <= 0) {
                    return null;
                }
                runners = createRunners();
                onRunStarted();
                ThreadSpawner spawner = spawnThreads(runners);
                spawner.awaitCompletion();
                return null;
            } finally {
                onRunCompleted();
            }
        };
    }

    private ThreadSpawner spawnThreads(TimeStepRunner[] runners) {
        ThreadSpawner spawner = new ThreadSpawner(testContext.getTestId());

        for (TimeStepRunner runner : runners) {
            String executionGroup = runner.executionGroup;
            String name = testContext.getTestId();
            if (!executionGroup.equals("")) {
                name += "-" + executionGroup;
            }
            name += "-timestepThread";
            spawner.spawn(name, runner);
        }

        return spawner;
    }

    @SuppressWarnings("unchecked")
    private TimeStepRunner[] createRunners() throws Exception {
        TimeStepRunner[] runners = new TimeStepRunner[totalThreadCount];

        int k = 0;
        for (String executionGroup : timeStepModel.getExecutionGroups()) {
            Class runnerClass = runnerClassMap.get(executionGroup);
            Constructor<TimeStepRunner> constructor = runnerClass
                    .getConstructor(testInstance.getClass(), TimeStepModel.class, String.class);

            MetronomeSupplier metronomeSupplier = metronomeSettingsMap.get(executionGroup);

            String rampupSecondsProperty = toPropertyName(executionGroup, "rampupSeconds");
            long rampupSeconds = binding.loadAsLong(rampupSecondsProperty, 0);
            if (rampupSeconds < 0) {
                throw new RuntimeException(rampupSecondsProperty + " can't be smaller than 0");
            }
            int threadCount = threadCountMap.get(executionGroup);
            long delayMs = SECONDS.toMillis(rampupSeconds) / threadCount;
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++) {
                TimeStepRunner runner = constructor.newInstance(testInstance, timeStepModel, executionGroup);
                runner.testContext = binding.getTestContext();
                runner.maxIterations = runIterationMap.get(executionGroup);
                runner.metronome = metronomeSupplier.get();
                runner.delayMillis = delayMs * threadIndex;
                runner.bind(binding);
                runners[k] = runner;
                k++;
            }
        }

        return runners;
    }
}
